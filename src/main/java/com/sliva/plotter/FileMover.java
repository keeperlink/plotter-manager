/*
 * GNU GENERAL PUBLIC LICENSE
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.GB;
import static com.sliva.plotter.IOUtils.KB;
import static com.sliva.plotter.LoggerUtil.log;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author Sliva Co
 */
public class FileMover {

    private static final long PRINT_PROGRESS_STEP_PERCENT = 10;
    private static final int SELECTIVE_CHECK_SECTORS = 200;
    private static final int SELECTIVE_CHECK_SECTOR_SIZE = 4 * KB;

    private final File sourceFile;
    private final File destinationDir;
    private final int copyThrottle;
    private final byte[] buffer;
    private final AtomicBoolean paused;
    private final AtomicBoolean interrupted;

    public FileMover(File sourceFile, File destinationDir, int copyThrottle, byte[] buffer, AtomicBoolean paused, AtomicBoolean interrupted) {
        this.sourceFile = sourceFile;
        this.destinationDir = destinationDir;
        this.copyThrottle = copyThrottle;
        this.buffer = buffer;
        this.paused = paused;
        this.interrupted = interrupted;
    }

    public void run() throws IOException, InterruptedException {
        move();
    }

    private void move() throws IOException, InterruptedException {
        if (!sourceFile.exists()) {
            throw new IOException("Unexpected: Source file not found: " + sourceFile);
        }
        long destinationTotalSpace = destinationDir.getTotalSpace();
        long destinationAvailableSpace = destinationDir.getUsableSpace();
        if (destinationTotalSpace > 0 && destinationAvailableSpace < sourceFile.length()) {
            throw new IOException("Destination drive is out of space: available=" + destinationAvailableSpace / GB + " GB, required=" + sourceFile.length() / GB + " GB");
        }
        String fileName = sourceFile.getName();
        String tempFileName = fileName + ".moving";
        File destinationTempFile = new File(destinationDir, tempFileName);
        log("Copying file (size: " + (sourceFile.length() / GB) + " GB) " + sourceFile + " ==> " + destinationTempFile);
        copyFile(sourceFile, destinationTempFile);
        log("Validating file " + destinationTempFile);
        validateFile(sourceFile, destinationTempFile);
        File destinationFile = new File(destinationDir, fileName);
        log("Renaming file " + destinationTempFile + " ==> " + destinationFile);
        destinationTempFile.renameTo(destinationFile);
        if (!destinationFile.exists()) {
            throw new IOException("Unexpected: Destination file not found: " + destinationFile);
        }
        destinationFile.setLastModified(sourceFile.lastModified());
        log("Deleting source file " + sourceFile);
        sourceFile.delete();
        if (sourceFile.exists()) {
            throw new IOException("Unexpected: Cannot delete Source file: " + sourceFile);
        }
    }

    @SuppressWarnings({"NestedAssignment", "UseSpecificCatch", "CallToPrintStackTrace", "SleepWhileInLoop"})
    private void copyFile(File source, File destination) throws IOException, InterruptedException {
        long fileSize = sourceFile.length();
        boolean unfinishedNeedToCleanup = false;
        try (InputStream is = new FileInputStream(source); OutputStream os = new FileOutputStream(destination)) {
            long copiedBytes = 0;
            long nextProgressToPrintPercent = PRINT_PROGRESS_STEP_PERCENT;
            up:
            for (int n = is.read(buffer); n > 0; n = is.read(buffer)) {
                while (paused.get()) {
                    Thread.sleep(50);
                    if (interrupted.get()) {
                        log("Interrupted at file copy on pause. Cleaning up...");
                        unfinishedNeedToCleanup = true;
                        break up;
                    }
                }
                os.write(buffer, 0, n);
                copiedBytes += n;
                long percent = copiedBytes * 100 / fileSize;
                if (percent >= nextProgressToPrintPercent) {
                    log("Copying file " + sourceFile + ": " + nextProgressToPrintPercent + "%");
                    nextProgressToPrintPercent += PRINT_PROGRESS_STEP_PERCENT;
                }
                if (copyThrottle > 0) {
                    Thread.sleep(copyThrottle);
                }
                if (interrupted.get()) {
                    log("Interrupted at file copy. Cleaning up...");
                    unfinishedNeedToCleanup = true;
                    break;
                }
            }
        }
        if (unfinishedNeedToCleanup) {
            log("Deleting unfinished file: " + destination.getAbsolutePath());
            destination.delete();
            throw new InterruptedException("Ctrl-C Interruption");
        }
    }

    private static void validateFile(File source, File destination) throws IOException {
        if (!destination.exists()) {
            throw new IOException("Unexpected: Destination temp file not found: " + destination);
        }
        if (destination.length() != source.length()) {
            throw new IOException("Unexpected: Destination temp file size mismatch: " + destination.length() + " <> " + source.length());
        }
        if (!selectiveCompareFiles(source, destination, SELECTIVE_CHECK_SECTORS, SELECTIVE_CHECK_SECTOR_SIZE)) {
            throw new IOException("Destination file validation failed");
        }
    }

    private static boolean selectiveCompareFiles(File file1, File file2, int checkSectors, int sectorSize) throws IOException {
        if (!file1.isFile() || !file2.isFile()) {
            log("Unexpected ERROR: Source or restination file doesn't exist");
            return false;
        }
        if (file1.length() != file2.length()) {
            log("Unexpected ERROR: File sizes are different");
            return false;
        }
        long fileSize = file1.length();
        long step = fileSize / checkSectors;
        byte[] buf1 = new byte[sectorSize];
        byte[] buf2 = new byte[buf1.length];
        try (RandomAccessFile raf1 = new RandomAccessFile(file1, "r"); RandomAccessFile raf2 = new RandomAccessFile(file2, "r")) {
            for (long pos = 0; pos < fileSize + step; pos += step) {
                raf1.seek(Math.min(pos, fileSize - buf1.length));
                int n1 = raf1.read(buf1);
                raf2.seek(Math.min(pos, fileSize - buf2.length));
                int n2 = raf2.read(buf2);
                if (n1 != n2 || !Arrays.equals(buf1, buf2)) {
                    log("Unexpected ERROR: File compare found a mismatch. pos=" + pos);
                    return false;
                }
            }
        }
        return true;
    }
}
