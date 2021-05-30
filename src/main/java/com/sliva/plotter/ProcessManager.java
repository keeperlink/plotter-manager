/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.MB;
import static com.sliva.plotter.IOUtils.fixVolumePathForWindows;
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
import static java.nio.file.StandardOpenOption.WRITE;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final String NO_WRITE_FILENAME = "no-write";
    private static final String TMP_PATH = "Chia.tmp";
    private static final Duration DELAY_MOVE = Duration.ofMinutes(30);
    private static final int COPY_BUFFER_SIZE = 10 * MB;

    private final File configFile;
    private int memory = 4000;
    private int nThreads = 8;
    private Duration delayStartQueue = Duration.ofMinutes(60);
    private final Set<File> inUseDirectDest = new HashSet<>();
    private final Set<String> runningProcessQueues = new HashSet<>();
    private final AtomicInteger queueCount = new AtomicInteger();
    private final AtomicInteger movingProcessesCount = new AtomicInteger();

    public ProcessManager(File configFile) {
        this.configFile = configFile;
    }

    @SuppressWarnings("SleepWhileInLoop")
    public void run() throws IOException, InterruptedException {
        log("ProcessManager: Using plotting log file: " + PLOTTING_LOG_FILE.getAbsolutePath());
        log("ProcessManager STARTED: Watching for stop file: " + STOP_FILE.getAbsolutePath());
        Collection<PlotterParams> config = readConfig(configFile);
        log("ProcessManager: Available destinations: " + getAvailableDestinations());
        config.forEach(this::createProcessQueue);
        Thread.sleep(2000);
        while (!runningProcessQueues.isEmpty() || movingProcessesCount.get() != 0) {
            Thread.sleep(10000);
            getAvailableDestinations();
        }
        log("ProcessManager FINISHED");
    }

    private void createProcessQueue(PlotterParams p) {
        int queueId = queueCount.getAndIncrement();
        log(p.getName() + " ProcessManager: Creating new process queue \"" + p.getName() + "\" #" + queueId);
        CompletableFuture.runAsync(() -> {
            Duration delay = Duration.ofMillis(delayStartQueue.toMillis() * queueId);
            if (delay.toMillis() > 0) {
                log(p.getName() + " ProcessManager: Delaying queue \"" + p.getName() + "\" #" + queueId + " for " + delay);
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ex) {
                    log(p.getName() + " ProcessManager: Interrupted during delay sleep queue \"" + p.getName() + "\" #" + queueId);
                    return;
                }
            }
            synchronized (runningProcessQueues) {
                runningProcessQueues.add(p.getName());
            }
            createProcess(p);
        });
    }

    private void destroyProcessQueue(PlotterParams p) {
        synchronized (runningProcessQueues) {
            runningProcessQueues.remove(p.getName());
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    private void createProcess(PlotterParams p) {
        if (STOP_FILE.exists()) {
            log(p.getName() + " ProcessManager: STOP file detected. Exiting queue \"" + p.getName() + "\"");
            destroyProcessQueue(p);
            return;
        }
        if (getAvailableDestinations().isEmpty()) {
            log(p.getName() + " ProcessManager: No destination space left. Exiting queue \"" + p.getName() + "\"");
            destroyProcessQueue(p);
            return;
        }
        log(p.getName() + " ProcessManager: Creating process: " + p.getName() + "\t" + p.getTmpDrive() + " -> " + p.tmp2Drive);
        try {
            boolean isTmp2Dest = "dest".equals(p.getTmp2Drive());
            File tmpPath = new File(fixVolumePathForWindows(p.getTmpDrive()), TMP_PATH);
            File tmp2Path;
            if (isTmp2Dest) {
                synchronized (inUseDirectDest) {
                    tmp2Path = getAvailableDestinations().stream().filter(f -> !inUseDirectDest.contains(f)).findAny().get();
                    inUseDirectDest.add(tmp2Path);
                }
            } else {
                tmp2Path = new File(fixVolumePathForWindows(p.getTmp2Drive()), TMP_PATH);
            }
            log(p.getName() + " ProcessManager: Starting process \"" + p.getName() + "\" " + p.getTmpDrive() + " -> " + p.getTmp2Drive() + ", isTmp2Dest=" + isTmp2Dest);
            new PlotProcess(p.getName(), tmpPath, tmp2Path, isTmp2Dest, memory, nThreads, pp -> onCompleteProcess(pp, p)).startProcess();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void onCompleteProcess(PlotProcess pp, PlotterParams p) {
        long runtime = System.currentTimeMillis() - pp.getCreateTimestamp();
        log(p.getName() + " ProcessManager: Process complete: \"" + pp.getName() + "\". Runtime: " + Duration.ofMillis(runtime));
        logPlottingStat(pp);
        if (pp.isTmp2Dest()) {
            synchronized (inUseDirectDest) {
                inUseDirectDest.remove(pp.getTmp2Path());
            }
        } else {
            moveFileAcync(new File(pp.getTmp2Path(), pp.getResultFileName()), p);
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
            Files.write(PLOTTING_LOG_FILE.toPath(), s.getBytes(StandardCharsets.UTF_8), WRITE, CREATE, APPEND);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void moveFileAcync(File srcFile, PlotterParams p) {
        movingProcessesCount.incrementAndGet();
        CompletableFuture.runAsync(() -> moveFile(srcFile, p));
    }

    @SuppressWarnings({"SleepWhileInLoop", "CallToPrintStackTrace", "SleepWhileHoldingLock"})
    private void moveFile(File srcFile, PlotterParams p) {
        try {
            if (!DELAY_MOVE.isZero()) {
                log(p.getName() + " ProcessManager: Delaying move for " + DELAY_MOVE + ". File: " + srcFile.getAbsolutePath());
                Thread.sleep(DELAY_MOVE.toMillis());
            }
            File dest;
            synchronized (inUseMoveDest) {
                for (;;) {
                    //find destination with enough space that is not currently used by another move process and prefer one that is not used as direct destination (temp2=dest)
                    Optional<File> odest = getAvailableDestinations().stream().filter(f -> !inUseMoveDest.contains(f)).sorted(Comparator.comparingInt(f -> inUseDirectDest.contains(f) ? 1 : 0)).findFirst();
                    if (odest.isPresent()) {
                        dest = odest.get();
                        break;
                    }
                    Thread.sleep(100);
                }
                inUseMoveDest.add(dest);
            }
            long s = System.currentTimeMillis();
            try {
                log(p.getName() + " ProcessManager: Move START. File " + srcFile.getAbsolutePath() + " to " + dest.getAbsolutePath());
                new FileMover(srcFile, dest, 0, new byte[COPY_BUFFER_SIZE], new AtomicBoolean(), new AtomicBoolean()).run();
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                synchronized (inUseMoveDest) {
                    inUseMoveDest.remove(dest);
                }
                log(p.getName() + " ProcessManager: Move FINISHED. Runtime: " + Duration.ofMillis(System.currentTimeMillis() - s) + ". File " + srcFile.getAbsolutePath() + " to " + dest.getAbsolutePath());
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            movingProcessesCount.decrementAndGet();
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
                            log("ProcessManager: readConfig: " + name + "\t " + tmpDrive + " -> " + tmp2Drive);
                            result.add(new PlotterParams(name, tmpDrive, tmp2Drive));
                        }
                    }
                } else if (s.startsWith("memory=")) {
                    memory = Integer.parseInt(s.split("=")[1].trim());
                    log("ProcessManager: readConfig: memory=" + memory);
                } else if (s.startsWith("threads=")) {
                    nThreads = Integer.parseInt(s.split("=")[1].trim());
                    log("ProcessManager: eadConfig: nThreads=" + nThreads);
                } else if (s.startsWith("delay=")) {
                    delayStartQueue = Duration.ofMinutes(Integer.parseInt(s.split("=")[1].trim()));
                    log("ProcessManager: readConfig: delayStartQueue=" + delayStartQueue);
                }
            }
        }
        return result;
    }

    private Collection<File> getAvailableDestinations() {
        Collection<File> result = Stream.of(File.listRoots())
                .filter(f -> !new File(f, NO_WRITE_FILENAME).exists())
                .map(f -> new File(f, DESTINATION_PATH))
                .filter(f -> f.exists() && f.isDirectory() && !new File(f, NO_WRITE_FILENAME).exists() && getFreeSpace(f) >= MIN_SPACE)
                .collect(Collectors.toList());
        updateCachedDestinations(result);
        return result;
    }

    private void updateCachedDestinations(Collection<File> result) {
        synchronized (cachedDestinations) {
            result.forEach(f -> {
                if (!cachedDestinations.contains(f)) {
                    log("ProcessManager: Detected new destination volume: " + f.getAbsolutePath());
                    cachedDestinations.add(f);
                }
            });
            for (Iterator<File> i = cachedDestinations.iterator(); i.hasNext();) {
                File f = i.next();
                if (!result.contains(f)) {
                    log("ProcessManager: Removing destination volume: " + f.getAbsolutePath());
                    i.remove();
                }
            }
        }
    }
    private final Set<File> cachedDestinations = new HashSet<>();

    private long getFreeSpace(File f) {
        synchronized (inUseDirectDest) {
            return f.getUsableSpace() - (inUseDirectDest.contains(f) ? MIN_SPACE : 0);
        }
    }

    private static class PlotterParams {

        private final String name;
        private final String tmpDrive;
        private final String tmp2Drive;

        public PlotterParams(String name, String tmpDrive, String tmp2Drive) {
            this.name = name;
            this.tmpDrive = tmpDrive;
            this.tmp2Drive = tmp2Drive;
        }

        public String getName() {
            return name;
        }

        public String getTmpDrive() {
            return tmpDrive;
        }

        public String getTmp2Drive() {
            return tmp2Drive;
        }

    }
}
