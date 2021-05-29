/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.AsyncUtil.asyncReadLines;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 * @author Sliva Co
 */
public class PlotProcess {

    public static final String TMP_FILE_EXT = ".tmp";
    public static final String EXEC_NAME = "chia.exe";
    public static final String LOG_DIR = "log";
    private static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final String name;
    private final File tmpPath;
    private final File tmp2Path;
    private final boolean tmp2Dest;
    private final int memSize;
    private final int nThreads;
    private final Consumer<PlotProcess> onComplete;
    private final File logDir;
    private String id;
    private String resultFileName;
    private final long createTimestamp = System.currentTimeMillis();
    private File logFile;
    private final StringBuilder outputBuffer = new StringBuilder();

    public PlotProcess(String name, File tmpPath, File tmp2Path, boolean tmp2Dest, int memSize, int nThreads, Consumer<PlotProcess> onComplete) {
        this.name = name;
        this.tmpPath = new File(tmpPath, name);
        this.tmp2Path = tmp2Dest ? tmp2Path : new File(tmp2Path, name);
        this.tmp2Dest = tmp2Dest;
        this.memSize = memSize;
        this.nThreads = nThreads;
        this.onComplete = onComplete;
        this.logDir = new File(LOG_DIR);
        this.logDir.mkdirs();
    }

    public String getName() {
        return name;
    }

    public File getTmpPath() {
        return tmpPath;
    }

    public File getTmp2Path() {
        return tmp2Path;
    }

    public boolean isTmp2Dest() {
        return tmp2Dest;
    }

    public String getId() {
        return id;
    }

    public String getResultFileName() {
        return resultFileName;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public Process startProcess() throws IOException {
        prepare();
        return runProcess();
    }

    private void prepare() throws IOException {
        tmpPath.mkdirs();
        tmp2Path.mkdirs();
        deleteTempFiles(tmpPath);
        if (!isTmp2Dest()) {
            deleteTempFiles(tmp2Path);
        }
    }

    private Process runProcess() throws IOException {
        File chiaExe = getChiaExecutable();
        Process proc = new ProcessBuilder()
                .command(chiaExe.getAbsolutePath(), getExecParamString())
                .directory(chiaExe.getParentFile())
                .start();
        asyncReadLines(proc.getInputStream(), StandardCharsets.UTF_8, this::onOutput);
        asyncReadLines(proc.getErrorStream(), StandardCharsets.UTF_8, this::onError);
        return proc;
    }

    private String getExecParamString() {
        return "plots create -k 32 -u 128 -n 1 -b " + memSize + " -r " + nThreads + " -t " + getTmpPath().getAbsolutePath() + " -2 " + getTmp2Path().getAbsolutePath() + " -d " + getTmp2Path().getAbsolutePath();
    }

    private void onOutput(String s) {
        if (s == null) {
            onComplete.accept(this);
        } else {
            log(s);
        }
    }

    private void onError(String s) {
        System.out.println(getName() + ": ERRROR: " + s);
    }

    private void log(String s) {
        Date date = new Date();
        if (s.startsWith("ID: ")) {
            id = s.substring(4);
        }
        if (logFile == null) {
            if (id != null) {
                logFile = new File(logDir, id + ".log");
                writeLog(outputBuffer.toString());
            }
        } else if (s.startsWith("Renamed final file ")) {
            resultFileName = s.split("\"")[3];
            File finalLogFile = new File(logDir, resultFileName + ".log");
            logFile.renameTo(finalLogFile);
            logFile = finalLogFile;
        }
        String logLine = DF.format(date) + ": " + s;
        if (logFile == null) {
            outputBuffer.append(logLine).append(System.lineSeparator());
        } else {
            writeLog(logLine + System.lineSeparator());
        }
    }

    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    private void writeLog(String s) {
        try {
            Files.writeString(logFile.toPath(), s, StandardCharsets.UTF_8, CREATE, APPEND);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static File getChiaExecutable() throws IOException {
        File chiaAppPath = new File(System.getenv("LOCALAPPDATA"), "chia-blockchain");
        return Stream.of(chiaAppPath.listFiles(f -> f.isDirectory() && f.getName().startsWith("app-")))
                .map(f -> getChiaExecutable(f))
                .filter(f -> f.exists())
                .findFirst()
                .orElseThrow(() -> new IOException(EXEC_NAME + " not found"));
    }

    private static File getChiaExecutable(File versionAppPath) {
        return new File(new File(new File(new File(versionAppPath, "resources"), "app.asar.unpacked"), "daemon"), EXEC_NAME);
    }

    private static void deleteTempFiles(File directory) {
        Stream.of(directory.listFiles(f -> f.isFile() && f.getName().endsWith(TMP_FILE_EXT))).forEach(f -> f.delete());
    }
}
