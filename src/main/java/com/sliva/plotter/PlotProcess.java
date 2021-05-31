/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.AsyncUtil.asyncReadLines;
import static com.sliva.plotter.IOUtils.deleteTempFiles;
import static com.sliva.plotter.LoggerUtil.getTimestampString;
import static com.sliva.plotter.LoggerUtil.log;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    public static final int PLOT_SIZE = 32;
    public static final int BUCKETS = 128;

    private static final String STARTING_PHASE = "Starting phase ";
    private static final String COMPUTING_TABLE = "Computing table ";
    private static final String BACKPROPAGATING_ON_TABLE = "Backpropagating on table ";
    private static final String COMPRESSING_TABLES = "Compressing tables ";
    private static final String BUCKET = "\tBucket ";
    private static final String FIRST_COMPUTATION_PASS = "\tFirst computation pass";
    private static final String SECOND_COMPUTATION_PASS = "\tSecond computation pass";

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
    private boolean started;
    private boolean finished;
    private int phase;
    private int subPhase;
    private int step;
    private int stepAddition;
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

    public boolean isStarted() {
        return started;
    }

    public boolean isFinished() {
        return finished;
    }

    public int getPhase() {
        return phase;
    }

    public int getSubPhase() {
        return subPhase;
    }

    public int getStep() {
        return step;
    }

    public Process startProcess() throws IOException {
        prepare();
        return runProcess();
    }

    private void prepare() throws IOException {
        getTmpPath().mkdirs();
        getTmp2Path().mkdirs();
        deleteTempFiles(getTmpPath(), TMP_FILE_EXT);
        if (!isTmp2Dest()) {
            deleteTempFiles(getTmp2Path(), TMP_FILE_EXT);
        }
    }

    private Process runProcess() throws IOException {
        File chiaExe = getChiaExecutable();
        Process proc = new ProcessBuilder()
                .command(getCommandWithParams(chiaExe))
                .directory(chiaExe.getParentFile())
                .redirectErrorStream(true)
                .start();
        asyncReadLines(proc.getInputStream(), StandardCharsets.UTF_8, this::onOutput);
        asyncReadLines(proc.getErrorStream(), StandardCharsets.UTF_8, this::onError);
        started = true;
        return proc;
    }

    private List<String> getCommandWithParams(File chiaExe) {
        return Arrays.asList(chiaExe.getAbsolutePath(), "plots", "create",
                "-k", Integer.toString(PLOT_SIZE),
                "-u", Integer.toString(BUCKETS),
                "-b", Integer.toString(memSize),
                "-r", Integer.toString(nThreads),
                "-t", getTmpPath().getAbsolutePath(),
                "-2", getTmp2Path().getAbsolutePath(),
                "-d", getTmp2Path().getAbsolutePath());
    }

    private void onOutput(String s) {
        try {
            if (s == null) {
                log(getName() + " PlotProcess.onOutput: Process finished. name=\"" + getName() + "\"");
                finished = true;
                CompletableFuture.runAsync(() -> onComplete.accept(this));
            } else {
                processStdOutLine(s);
            }
        } catch (Throwable t) {
            System.out.println("ERROR: " + t.getClass() + ": " + t.getMessage());
        }
    }

    private void onError(String s) {
        try {
            if (s != null) {
                log(getName() + " PlotProcess.onError: StdErr: " + s);
            }
        } catch (Throwable t) {
            System.out.println("ERROR: " + t.getClass() + ": " + t.getMessage());
        }
    }

    private void processStdOutLine(String s) {
        String logLine = getTimestampString() + ": " + s;
        if (s.startsWith("ID: ")) {
            id = s.substring(4);
            log(getName() + " PlotProcess: ID=" + getId());
        } else if (s.startsWith(STARTING_PHASE)) {
            phase = Integer.parseInt(s.substring(STARTING_PHASE.length(), STARTING_PHASE.length() + 1));
            subPhase = 0;
            step = 0;
        } else if (s.startsWith(COMPUTING_TABLE)) {
            subPhase = Integer.parseInt(s.substring(COMPUTING_TABLE.length()));
            step = 0;
        } else if (s.startsWith(BACKPROPAGATING_ON_TABLE)) {
            subPhase = 8 - Integer.parseInt(s.substring(BACKPROPAGATING_ON_TABLE.length()));
            step = 0;
        } else if (s.startsWith(COMPRESSING_TABLES)) {
            subPhase = Integer.parseInt(s.substring(COMPRESSING_TABLES.length(), COMPRESSING_TABLES.length() + 1));
            step = 0;
        } else if (s.startsWith(BUCKET)) {
            step = Integer.parseInt(s.split(" ")[1]) + stepAddition;
        } else if (s.startsWith(FIRST_COMPUTATION_PASS)) {
            stepAddition = BUCKETS;
        } else if (s.startsWith(SECOND_COMPUTATION_PASS)) {
            stepAddition = 0;
        }
        if (logFile == null) {
            if (getId() != null) {
                logFile = new File(logDir, getId() + ".log");
                //log(getName() + " PlotProcess. Using log file: " + logFile.getAbsolutePath());
                writeLog(outputBuffer.toString());
                outputBuffer.setLength(0);
            }
        } else if (s.startsWith("Renamed final file ")) {
            resultFileName = new File(s.split("\"")[3].replaceAll("\\\\\\\\", "\\\\")).getName();
            log(getName() + " PlotProcess: Result file name: " + getResultFileName());
            File finalLogFile = new File(logDir, getResultFileName() + ".log");
            //log(getName() + " PlotProcess: Renaming log file: " + logFile.getAbsolutePath() + " ==> " + finalLogFile.getAbsolutePath());
            logFile.renameTo(finalLogFile);
            logFile = finalLogFile;
        }
        if (logFile == null) {
            outputBuffer.append(logLine).append(System.lineSeparator());
        } else {
            writeLog(logLine + System.lineSeparator());
        }
    }

    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    private void writeLog(String s) {
        try {
            Files.write(logFile.toPath(), s.getBytes(StandardCharsets.UTF_8), WRITE, CREATE, APPEND);
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
}
