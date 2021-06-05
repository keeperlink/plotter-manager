/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.MB;
import static com.sliva.plotter.LoggerUtil.log;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 *
 * @author Sliva Co
 */
public class AsyncMover {

    private static final int COPY_BUFFER_SIZE = 10 * MB;
    private final Set<MovingProcess> movingProcesses = new HashSet<>();
    private final Set<File> inUseMoveDest = new HashSet<>();

    public int countMovingProcesses() {
        synchronized (movingProcesses) {
            return movingProcesses.size();
        }
    }

    public int countMovingProcessesNoDestination() {
        synchronized (movingProcesses) {
            return (int) movingProcesses.stream().filter(mp -> !mp.getDestinationPath().isPresent()).count();
        }
    }

    public Optional<MovingProcess> getMovingProcessByDestination(File destinationPath) {
        synchronized (movingProcesses) {
            return movingProcesses.stream().filter(mp -> mp.getDestinationPath().map(dp -> dp.equals(destinationPath)).orElse(false)).findAny();
        }
    }

    public void moveFileAcync(File srcFile, String queueName, Supplier<Collection<File>> availableDestinations, Duration delayMove) {
        MovingProcess mp = new MovingProcess(queueName, srcFile);
        synchronized (movingProcesses) {
            movingProcesses.add(mp);
        }
        CompletableFuture.runAsync(() -> moveFile(mp, availableDestinations, delayMove));
    }

    private void moveFile(MovingProcess mp, Supplier<Collection<File>> availableDestinations, Duration delayMove) {
        try {
            if (delayMove.toMillis() > 0) {
                log(mp.getQueueName() + " ProcessManager: Delaying move for " + delayMove + ". File: " + mp.getSrcFile().getAbsolutePath());
                Thread.sleep(delayMove.toMillis());
            }
            File dest;
            synchronized (inUseMoveDest) {
                for (;;) {
                    //find destination with enough space that is not currently used by another move process and prefer one that is not used as direct destination (temp2=dest)
                    Optional<File> odest = availableDestinations.get().stream().filter(f -> !inUseMoveDest.contains(f)).findFirst();
                    if (odest.isPresent()) {
                        dest = odest.get();
                        inUseMoveDest.add(dest);
                        break;
                    }
                    log(mp.getQueueName() + " ProcessManager: No any destination volume available at the moment. Waiting...");
                    inUseMoveDest.wait(Duration.ofMinutes(5).toMillis());
                }
                mp.setDestinationPath(Optional.of(dest));
            }
            long s = System.currentTimeMillis();
            try {
                log(mp.getQueueName() + " ProcessManager: Move START. File " + mp.getSrcFile().getAbsolutePath() + " to " + dest.getAbsolutePath());
                new FileMover(mp.getSrcFile(), dest, 0, new byte[COPY_BUFFER_SIZE], new AtomicBoolean(), new AtomicBoolean(), mp::setMovedBytes).run();
            } catch (IOException ex) {
                log(mp.getQueueName() + " ProcessManager: moveFile ERROR: " + ex.getClass() + ": " + ex.getMessage());
            } finally {
                synchronized (inUseMoveDest) {
                    inUseMoveDest.remove(dest);
                    inUseMoveDest.notifyAll();
                }
                log(mp.getQueueName() + " ProcessManager: Move FINISHED. Runtime: " + Duration.ofMillis(System.currentTimeMillis() - s) + ". File " + mp.getSrcFile().getAbsolutePath() + " to " + dest.getAbsolutePath());
            }
        } catch (InterruptedException ex) {
            log(mp.getQueueName() + " ProcessManager: moveFile interrupted: " + ex.getMessage());
        } finally {
            synchronized (movingProcesses) {
                movingProcesses.remove(mp);
            }
        }
    }

    public static class MovingProcess {

        private final String queueName;
        private final File srcFile;
        private Optional<File> destinationPath = Optional.empty();
        private final long fileSize;
        private long movedBytes;

        public MovingProcess(String queueName, File srcFile) {
            this.queueName = queueName;
            this.srcFile = srcFile;
            this.fileSize = srcFile.length();
        }

        public String getQueueName() {
            return queueName;
        }

        public File getSrcFile() {
            return srcFile;
        }

        public Optional<File> getDestinationPath() {
            return destinationPath;
        }

        public long getFileSize() {
            return fileSize;
        }

        public long getMovedBytes() {
            return movedBytes;
        }

        private void setDestinationPath(Optional<File> destinationPath) {
            this.destinationPath = destinationPath;
        }

        private void setMovedBytes(long movedBytes) {
            this.movedBytes = movedBytes;
        }

    }

}
