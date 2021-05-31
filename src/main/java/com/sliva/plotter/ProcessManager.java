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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
    private static final int COPY_BUFFER_SIZE = 10 * MB;

    private final File configFile;
    private int memory = 3500;
    private int nThreads = 4;
    private Duration delayStartQueue = Duration.ofMinutes(60);
    private Duration moveDelay = Duration.ofMinutes(30);
    private final Map<String, PlotterParams> plotterParamsMap = new HashMap<>();
    private final Set<File> inUseDirectDest = new HashSet<>();
    private final Set<String> runningProcessQueues = new HashSet<>();
    private final AtomicInteger queueCount = new AtomicInteger();
    private final AtomicInteger movingProcessesCount = new AtomicInteger();

    public ProcessManager(File configFile) {
        this.configFile = configFile;
    }

    @SuppressWarnings("SleepWhileInLoop")
    public void run() throws InterruptedException {
        log("ProcessManager: Using plotting log file: " + PLOTTING_LOG_FILE.getAbsolutePath());
        log("ProcessManager STARTED: Watching for stop file: " + STOP_FILE.getAbsolutePath());
        Set<File> refDestSet = new HashSet<>(getAvailableDestinations());
        readConfig(configFile);
        log("ProcessManager: Available destinations: " + getAvailableDestinations());
        getQueueNames().forEach(this::createProcessQueue);
        Thread.sleep(2000);
        while (!runningProcessQueues.isEmpty() || movingProcessesCount.get() != 0) {
            Thread.sleep(5000);
            if (readConfig(configFile) || checkChangedAndUpdate(getAvailableDestinations(), refDestSet)) {
                //restart non-running queues on any change in either config file or destination volumes availability
                getQueueNames().stream().filter(name -> !isQueueRunning(name)).forEach(this::createProcessQueue);
            }
        }
        log("ProcessManager FINISHED");
    }

    private void createProcessQueue(String queueName) {
        int queueId = queueCount.getAndIncrement();
        log(queueName + " ProcessManager: Creating new process queue \"" + queueName + "\" #" + queueId);
        CompletableFuture.runAsync(() -> {
            Duration delay = Duration.ofMillis(delayStartQueue.toMillis() * queueId);
            if (delay.toMillis() > 0) {
                log(queueName + " ProcessManager: Delaying queue \"" + queueName + "\" #" + queueId + " for " + delay);
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ex) {
                    log(queueName + " ProcessManager: Interrupted during delay sleep queue \"" + queueName + "\" #" + queueId);
                    return;
                }
            }
            synchronized (runningProcessQueues) {
                runningProcessQueues.add(queueName);
            }
            createProcess(queueName);
        });
    }

    private void destroyProcessQueue(String queueName) {
        synchronized (runningProcessQueues) {
            runningProcessQueues.remove(queueName);
        }
    }

    private boolean isQueueRunning(String queueName) {
        synchronized (runningProcessQueues) {
            return runningProcessQueues.contains(queueName);
        }
    }

    private void createProcess(String queueName) {
        if (STOP_FILE.exists()) {
            log(queueName + " ProcessManager: STOP file detected (" + STOP_FILE.getAbsolutePath() + "). Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        if (getAvailableDestinations().isEmpty()) {
            log(queueName + " ProcessManager: No destination space left. Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        PlotterParams p = getPlotterParams(queueName);
        if (p == null) {
            log(queueName + " ProcessManager: Queue removed from config. Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        log(p.getName() + " ProcessManager: Creating process: " + p.getName() + "\t" + p.getTmpDrive() + " -> " + p.tmp2Drive);
        try {
            boolean isTmp2Dest = "dest".equals(p.getTmp2Drive());
            File tmpPath = new File(fixVolumePathForWindows(p.getTmpDrive()), TMP_PATH);
            File tmp2Path;
            if (isTmp2Dest) {
                synchronized (inUseDirectDest) {
                    Optional<File> otmp2Path = getAvailableDestinations().stream().filter(f -> !inUseDirectDest.contains(f)).findAny();
                    if (!otmp2Path.isPresent()) {
                        log(p.getName() + " ProcessManager: No available volumes for direct destination. All available destination volumes: " + getAvailableDestinations() + ", In-use by other processes destination volumes: " + inUseDirectDest + ". Exiting queue \"" + p.getName() + "\"");
                        destroyProcessQueue(queueName);
                        return;
                    }
                    tmp2Path = otmp2Path.get();
                    log(p.getName() + " ProcessManager: Reserving volume for direct destination: " + tmp2Path);
                    inUseDirectDest.add(tmp2Path);
                }
            } else {
                tmp2Path = new File(fixVolumePathForWindows(p.getTmp2Drive()), TMP_PATH);
            }
            log(p.getName() + " ProcessManager: Starting process \"" + p.getName() + "\" " + p.getTmpDrive() + " -> " + p.getTmp2Drive() + ", isTmp2Dest=" + isTmp2Dest + ", tmpPath=" + tmpPath + ", tmp2Path=" + tmp2Path);
            new PlotProcess(p.getName(), tmpPath, tmp2Path, isTmp2Dest, memory, nThreads, pp -> onCompleteProcess(pp, queueName)).startProcess();
        } catch (IOException ex) {
            log(p.getName() + " ProcessManager: ERROR: " + ex.getClass() + ": " + ex.getMessage());
            destroyProcessQueue(queueName);
        }
    }

    private void onCompleteProcess(PlotProcess pp, String queueName) {
        long runtime = System.currentTimeMillis() - pp.getCreateTimestamp();
        log(queueName + " ProcessManager: Process complete: \"" + pp.getName() + "\". Runtime: " + Duration.ofMillis(runtime));
        logPlottingStat(pp);
        if (pp.isTmp2Dest()) {
            synchronized (inUseDirectDest) {
                inUseDirectDest.remove(pp.getTmp2Path());
            }
        } else {
            boolean delayMove = pp.getTmp2Path().equals(pp.getTmpPath());
            moveFileAcync(new File(pp.getTmp2Path(), pp.getResultFileName()), queueName, delayMove);
        }
        createProcess(queueName);
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

    private void moveFileAcync(File srcFile, String queueName, boolean delayMove) {
        movingProcessesCount.incrementAndGet();
        CompletableFuture.runAsync(() -> moveFile(srcFile, queueName, delayMove));
    }

    @SuppressWarnings({"SleepWhileInLoop", "SleepWhileHoldingLock"})
    private void moveFile(File srcFile, String queueName, boolean delayMove) {
        try {
            if (delayMove && !moveDelay.isZero()) {
                log(queueName + " ProcessManager: Delaying move for " + moveDelay + ". File: " + srcFile.getAbsolutePath());
                Thread.sleep(moveDelay.toMillis());
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
                    Thread.sleep(1000);
                }
                inUseMoveDest.add(dest);
            }
            long s = System.currentTimeMillis();
            try {
                log(queueName + " ProcessManager: Move START. File " + srcFile.getAbsolutePath() + " to " + dest.getAbsolutePath());
                new FileMover(srcFile, dest, 0, new byte[COPY_BUFFER_SIZE], new AtomicBoolean(), new AtomicBoolean()).run();
            } catch (IOException ex) {
                log(queueName + " ProcessManager: moveFile ERROR: " + ex.getClass() + ": " + ex.getMessage());
            } finally {
                synchronized (inUseMoveDest) {
                    inUseMoveDest.remove(dest);
                }
                log(queueName + " ProcessManager: Move FINISHED. Runtime: " + Duration.ofMillis(System.currentTimeMillis() - s) + ". File " + srcFile.getAbsolutePath() + " to " + dest.getAbsolutePath());
            }
        } catch (InterruptedException ex) {
            log(queueName + " ProcessManager: moveFile interrupted: " + ex.getMessage());
        } finally {
            movingProcessesCount.decrementAndGet();
        }
    }
    private final Set<File> inUseMoveDest = new HashSet<>();

    private Collection<String> getQueueNames() {
        synchronized (plotterParamsMap) {
            return new HashSet<>(plotterParamsMap.keySet());
        }
    }

    private PlotterParams getPlotterParams(String queueName) {
        synchronized (plotterParamsMap) {
            return plotterParamsMap.get(queueName);
        }
    }

    /**
     * Read configuration file.
     *
     * @param file Config file
     * @return true if config has been changed since last read
     */
    private boolean readConfig(File file) {
        boolean changed = false;
        Collection<PlotterParams> ppFromConfig = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            synchronized (plotterParamsMap) {
                for (String s = in.readLine(); s != null; s = in.readLine()) {
                    if (s.startsWith("#")) {
                        //skip
                    } else if (s.contains(" -> ")) {
                        String a[] = s.split("\t");
                        if (a.length >= 2) {
                            String name = a[0];
                            String b[] = a[1].split(" -> ");
                            if (b.length == 2) {
                                String tmpDrive = b[0];
                                String tmp2Drive = b[1];
                                PlotterParams pp = new PlotterParams(name, tmpDrive, tmp2Drive);
                                PlotterParams ppOld = plotterParamsMap.get(name);
                                if (ppOld == null || !ppOld.equals(pp)) {
                                    plotterParamsMap.put(name, pp);
                                    changed = true;
                                    log("ProcessManager: readConfig: " + name + "\t " + tmpDrive + " -> " + tmp2Drive);
                                }
                                ppFromConfig.add(pp);
                            }
                        }
                    } else if (s.startsWith("memory=")) {
                        int memory2 = Integer.parseInt(s.split("=")[1].trim());
                        if (memory2 != memory) {
                            memory = memory2;
                            changed = true;
                            log("ProcessManager: readConfig: memory=" + memory);
                        }
                    } else if (s.startsWith("threads=")) {
                        int nThreads2 = Integer.parseInt(s.split("=")[1].trim());
                        if (nThreads2 != nThreads) {
                            nThreads = nThreads2;
                            changed = true;
                            log("ProcessManager: eadConfig: nThreads=" + nThreads);
                        }
                    } else if (s.startsWith("delay=")) {
                        Duration delayStartQueue2 = Duration.ofMinutes(Integer.parseInt(s.split("=")[1].trim()));
                        if (!delayStartQueue2.equals(delayStartQueue)) {
                            delayStartQueue = delayStartQueue2;
                            changed = true;
                            log("ProcessManager: readConfig: delayStartQueue=" + delayStartQueue);
                        }
                    } else if (s.startsWith("move-delay=")) {
                        Duration moveDelay2 = Duration.ofMinutes(Integer.parseInt(s.split("=")[1].trim()));
                        if (!moveDelay2.equals(moveDelay)) {
                            moveDelay = moveDelay2;
                            changed = true;
                            log("ProcessManager: readConfig: moveDelay=" + moveDelay);
                        }
                    }
                }
                for (Iterator<Entry<String, PlotterParams>> i = plotterParamsMap.entrySet().iterator(); i.hasNext();) {
                    Entry<String, PlotterParams> e = i.next();
                    Optional<PlotterParams> opp = ppFromConfig.stream().filter(pp -> pp.getName().equals((e.getKey()))).findFirst();
                    if (opp.isPresent()) {
                        if (!opp.get().equals(e.getValue())) {
                            log("ProcessManager: readConfig: Detected Queue config change: " + e.getKey()
                                    + ". Old: " + e.getValue().getTmpDrive() + " -> " + e.getValue().getTmp2Drive()
                                    + ". New: " + opp.get().getTmpDrive() + " -> " + opp.get().getTmp2Drive() + ".");
                            e.setValue(opp.get());
                            changed = true;
                        }
                    } else {
                        log("ProcessManager: readConfig: Detected Queue config removal: " + e.getKey());
                        i.remove();
                        changed = true;
                    }
                }
            }
        } catch (IOException ex) {
            log("ProcessManager: readConfig: ERROR: " + ex.getClass() + " " + ex.getMessage());
        }
        return changed;
    }

    private Collection<File> getAvailableDestinations() {
        Collection<File> result = Stream.of(File.listRoots())
                .filter(f -> !new File(f, NO_WRITE_FILENAME).exists())
                .map(f -> new File(f, DESTINATION_PATH))
                .filter(f -> f.exists() && f.isDirectory() && !new File(f, NO_WRITE_FILENAME).exists() && getFreeSpace(f) >= MIN_SPACE)
                .collect(Collectors.toList());
        return result;
    }

    private boolean checkChangedAndUpdate(Collection<File> newData, Set<File> oldData) {
        AtomicBoolean changed = new AtomicBoolean(false);
        synchronized (oldData) {
            newData.forEach(f -> {
                if (!oldData.contains(f)) {
                    log("ProcessManager: Detected new destination volume: " + f.getAbsolutePath());
                    oldData.add(f);
                    changed.set(true);
                }
            });
            for (Iterator<File> i = oldData.iterator(); i.hasNext();) {
                File f = i.next();
                if (!newData.contains(f)) {
                    log("ProcessManager: Removing destination volume: " + f.getAbsolutePath());
                    i.remove();
                    changed.set(true);
                }
            }
        }
        return changed.get();
    }

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

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 41 * hash + Objects.hashCode(this.name);
            hash = 41 * hash + Objects.hashCode(this.tmpDrive);
            hash = 41 * hash + Objects.hashCode(this.tmp2Drive);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final PlotterParams other = (PlotterParams) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (!Objects.equals(this.tmpDrive, other.tmpDrive)) {
                return false;
            }
            if (!Objects.equals(this.tmp2Drive, other.tmp2Drive)) {
                return false;
            }
            return true;
        }
    }
}
