/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.MB;
import static com.sliva.plotter.LoggerUtil.getTimestampString;
import static com.sliva.plotter.LoggerUtil.log;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Sliva Co
 */
public class ProcessManager {

    private static final long MIN_SPACE = 109_000_000_000L;
    private static final File STOP_FILE = new File("plotting-stop");
    private static final File PLOTTING_LOG_FILE = new File("plotting.log");
    private static final String DESTINATION_PATH = "Chia.plot";
    private static final String TMP_PATH = "Chia.tmp";
    private static final int COPY_BUFFER_SIZE = 10 * MB;

    private final File configFile;
    private int memory = 4000;
    private int nThreads = 8;
    private final Set<File> inUseDirectDest = new HashSet<>();
    private final Set<String> runningProcessQueues = new HashSet<>();

    public ProcessManager(File configFile) {
        this.configFile = configFile;
    }

    @SuppressWarnings("SleepWhileInLoop")
    public void run() throws IOException, InterruptedException {
        log("ProcessManager STARTED: Watching for stop file: " + STOP_FILE.getAbsolutePath());
        readConfig(configFile).forEach(this::createProcessQueue);
        while (!runningProcessQueues.isEmpty()) {
            Thread.sleep(1000);
        }
        log("ProcessManager FINISHED");
    }

    private void createProcessQueue(PlotterParams p) {
        log("ProcessManager: Creating new process queue \"" + p.name + "\"");
        synchronized (runningProcessQueues) {
            runningProcessQueues.add(p.name);
        }
        createProcess(p);
    }

    private void destroyProcessQueue(PlotterParams p) {
        synchronized (runningProcessQueues) {
            runningProcessQueues.remove(p.name);
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    private void createProcess(PlotterParams p) {
        if (STOP_FILE.exists()) {
            log("ProcessManager: STOP file detected. Exiting queue \"" + p.name + "\"");
            destroyProcessQueue(p);
            return;
        }
        if (getAvailableDestinations().isEmpty()) {
            log("ProcessManager: No destination space left. Exiting queue \"" + p.name + "\"");
            destroyProcessQueue(p);
            return;
        }
        log("ProcessManager: Creating process: " + p.name + "\t" + p.tmpDrive + " -> " + p.tmp2Drive);
        try {
            boolean isTmp2Dest = "dest".equals(p.tmp2Drive);
            File tmpPath = new File(p.tmpDrive, TMP_PATH);
            File tmp2Path;
            if (isTmp2Dest) {
                synchronized (inUseDirectDest) {
                    tmp2Path = getAvailableDestinations().stream().filter(f -> !inUseDirectDest.contains(f)).findAny().get();
                    inUseDirectDest.add(tmp2Path);
                }
            } else {
                tmp2Path = new File(p.tmp2Drive, TMP_PATH);
            }
            log("ProcessManager: Starting process \"" + p.name + "\" " + p.tmpDrive + " -> " + p.tmp2Drive + ", isTmp2Dest=" + isTmp2Dest);
            new PlotProcess(p.name, tmpPath, tmp2Path, isTmp2Dest, memory, nThreads, pp -> onCompleteProcess(pp, p)).startProcess();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void onCompleteProcess(PlotProcess pp, PlotterParams p) {
        long runtime = System.currentTimeMillis() - pp.getCreateTimestamp();
        log("ProcessManager: Process complete: \"" + pp.getName() + "\". Runtime: " + Duration.ofMillis(runtime));
        logPlottingStat(pp);
        if (pp.isTmp2Dest()) {
            synchronized (inUseDirectDest) {
                inUseDirectDest.remove(pp.getTmp2Path());
            }
        } else {
            CompletableFuture.runAsync(() -> moveFile(new File(pp.getTmp2Path(), pp.getResultFileName())));
        }
        createProcess(p);
    }

    @SuppressWarnings({"CallToPrintStackTrace", "UseSpecificCatch"})
    private void logPlottingStat(PlotProcess pp) {
        try {
            String s = getTimestampString() + " " + Duration.ofMillis(System.currentTimeMillis() - pp.getCreateTimestamp())
                    + "\t" + pp.getName() + " " + pp.getTmpPath() + " -> " + pp.getTmp2Path()
                    + " " + pp.getId() + " " + pp.getResultFileName()
                    + System.lineSeparator();
            Files.writeString(PLOTTING_LOG_FILE.toPath(), s, StandardCharsets.UTF_8, CREATE, APPEND);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @SuppressWarnings({"NestedAssignment", "SleepWhileInLoop", "CallToPrintStackTrace", "SleepWhileHoldingLock"})
    private void moveFile(File srcFile) {
        try {
            File dest;
            Optional<File> odest;
            synchronized (inUseMoveDest) {
                while (!(odest = getAvailableDestinations().stream().filter(f -> !inUseMoveDest.contains(f)).findAny()).isPresent()) {
                    Thread.sleep(100);
                }
                dest = odest.get();
                inUseMoveDest.add(dest);
            }
            long s = System.currentTimeMillis();
            try {
                log("Move START. File " + srcFile.getAbsolutePath() + " to " + dest.getAbsolutePath());
                new FileMover(srcFile, dest, 0, new byte[COPY_BUFFER_SIZE], new AtomicBoolean(), new AtomicBoolean()).run();
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                synchronized (inUseMoveDest) {
                    inUseMoveDest.remove(dest);
                }
                log("Move FINISHED. Runtime: " + Duration.ofMillis(System.currentTimeMillis() - s) + ". File " + srcFile.getAbsolutePath() + " to " + dest.getAbsolutePath());
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
    private final Set<File> inUseMoveDest = new HashSet<>();

    private Collection<PlotterParams> readConfig(File file) throws IOException {
        Collection<PlotterParams> result = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            for (String s = in.readLine(); s != null; s = in.readLine()) {
                if (s.contains(" -> ")) {
                    String a[] = s.split("\t");
                    if (a.length >= 2) {
                        String name = a[0];
                        String b[] = a[1].split(" -> ");
                        if (b.length == 2) {
                            String tmpDrive = b[0];
                            String tmp2Drive = b[1];
                            log("readConfig: " + name + "\t " + tmpDrive + " -> " + tmp2Drive);
                            result.add(new PlotterParams(name, tmpDrive, tmp2Drive));
                        }
                    }
                } else if (s.startsWith("memory=")) {
                    memory = Integer.parseInt(s.split("=")[1].trim());
                    log("readConfig: memory=" + memory);
                } else if (s.startsWith("threads=")) {
                    nThreads = Integer.parseInt(s.split("=")[1].trim());
                    log("readConfig: nThreads=" + nThreads);
                }
            }
        }
        return result;
    }

    private Collection<File> getAvailableDestinations() {
        return Stream.of(File.listRoots())
                .map(f -> new File(f, DESTINATION_PATH))
                .filter(f -> f.exists() && f.isDirectory() && getFreeSpace(f) >= MIN_SPACE)
                .collect(Collectors.toUnmodifiableList());
    }

    private long getFreeSpace(File f) {
        synchronized (inUseDirectDest) {
            return f.getUsableSpace() - (inUseDirectDest.contains(f) ? MIN_SPACE : 0);
        }
    }

    private static class PlotterParams {

        final String name;
        final String tmpDrive;
        final String tmp2Drive;

        public PlotterParams(String name, String tmpDrive, String tmp2Drive) {
            this.name = name;
            this.tmpDrive = tmpDrive;
            this.tmp2Drive = tmp2Drive;
        }

    }

}
