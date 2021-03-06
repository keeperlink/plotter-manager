/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.checkChangedAndUpdate;
import static com.sliva.plotter.IOUtils.fixVolumePathForWindows;
import static com.sliva.plotter.IOUtils.isNetworkDriveCached;
import static com.sliva.plotter.LoggerUtil.getTimestampString;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Sliva Co
 */
public class ProcessManager {

    private static final String VERSION = "1.0.13";
    private static final long MIN_SPACE = 109_000_000_000L;
    private static final File STOP_FILE = new File("plotting-stop");
    private static final File PLOTTING_LOG_FILE = new File("plotting.log");
    private static final String DESTINATION_PATH = "Chia.plot";
    private static final String NO_WRITE_FILENAME = "no-write";
    private static final String NO_DIRECT_FILENAME = "no-direct";
    private static final String TMP_PATH = "Chia.tmp";
    private static final Duration CHECK_PERIOD = Duration.ofSeconds(5);

    private final File configFile;
    private final Config config = new Config();
    private final AsyncMover asyncMover = new AsyncMover();
    private final Set<File> inUseDirectDest = new HashSet<>();
    private final Map<String, Optional<PlotProcess>> runningProcessQueues = new HashMap<>();

    public ProcessManager(File configFile) {
        this.configFile = configFile;
    }

    @SuppressWarnings("SleepWhileInLoop")
    public void run() throws InterruptedException {
        log(null, "STARTED");
        if (STOP_FILE.exists()) {
            //rename stop-file left from previous execution stop request
            STOP_FILE.renameTo(new File(STOP_FILE.getAbsolutePath() + "_N"));
        }
        Set<File> cachedDestSet = new HashSet<>(getAvailableDestinations());
        ConfigReader.readConfig(configFile, config);
        log(null, "Available destinations: " + getAvailableDestinations());
        config.getQueueNames().forEach(this::createProcessQueue);
        while (!runningProcessQueues.isEmpty() || asyncMover.countMovingProcesses() != 0) {
            Thread.sleep(CHECK_PERIOD.toMillis());
            if (ConfigReader.readConfig(configFile, config) || checkChangedAndUpdate(getAvailableDestinations(), cachedDestSet, this::onRootChanged)) {
                //restart non-running queues on any change in either config file or destination volumes availability
                config.getQueueNames().stream().filter(q -> !isQueueRunning(q))
                        .forEach(this::recreateProcessQueue);
            }
        }
        log(null, "FINISHED");
    }

    private void createProcessQueue(String queueName) {
        createProcessQueue(queueName, true);
    }

    private void recreateProcessQueue(String queueName) {
        createProcessQueue(queueName, false);
    }

    private void createProcessQueue(String queueName, boolean doDelay) {
        if (STOP_FILE.exists()) {
            return;
        }
        int numOldRunningQueues;
        synchronized (runningProcessQueues) {
            if (runningProcessQueues.containsKey(queueName)) {
                log(queueName, "Queue already exists - exiting");
                return;
            }
            numOldRunningQueues = runningProcessQueues.size();
            runningProcessQueues.put(queueName, Optional.empty());
        }
        log(queueName, "Creating new process queue");
        CompletableFuture.runAsync(() -> {
            Duration delay = config.getDelayStartQueue().multipliedBy(doDelay ? numOldRunningQueues : 0);
            if (delayStartQueue(queueName, delay)) {
                createProcess(queueName);
            } else {
                destroyProcessQueue(queueName);
            }
        });
    }

    @SuppressWarnings("SleepWhileInLoop")
    private boolean delayStartQueue(String queueName, Duration delay) {
        if (delay.toMillis() > 0) {
            log(queueName, "Delaying queue for " + delay);
            try {
                for (long timeout = System.currentTimeMillis() + delay.toMillis(); System.currentTimeMillis() < timeout;) {
                    if (STOP_FILE.exists()) {
                        log(queueName, "STOP file detected (" + STOP_FILE.getAbsolutePath() + "). Interrupting queue delay loop");
                        return false;
                    } else if (!hasDestinationSpace()) {
                        log(queueName, "No destination space left. Exiting queue \"" + queueName + "\"");
                        return false;
                    }
                    Thread.sleep(Duration.ofSeconds(10).toMillis());
                }
            } catch (InterruptedException ex) {
                log(queueName, "Interrupted during delay sleep queue");
                return false;
            }
        }
        return true;
    }

    private void destroyProcessQueue(String queueName) {
        synchronized (runningProcessQueues) {
            runningProcessQueues.remove(queueName);
        }
    }

    private boolean isQueueRunning(String queueName) {
        synchronized (runningProcessQueues) {
            return runningProcessQueues.containsKey(queueName);
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    private void createProcess(String queueName) {
        if (STOP_FILE.exists()) {
            log(queueName, "STOP file detected (" + STOP_FILE.getAbsolutePath() + "). Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        if (!hasDestinationSpace()) {
            log(queueName, "No destination space left. Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        PlotterParams p = config.getPlotterParams(queueName);
        if (p == null) {
            log(queueName, "Queue removed from config. Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        log(queueName, "Creating process: " + queueName + "\t" + p.getTmpDrive() + " -> " + p.getTmp2Drive());
        try {
            boolean isTmp2Dest = "dest".equals(p.getTmp2Drive());
            File tmpPath = new File(fixVolumePathForWindows(p.getTmpDrive()), TMP_PATH);
            File tmp2Path;
            if (isTmp2Dest) {
                synchronized (inUseDirectDest) {
                    Optional<File> otmp2Path = getDirectDestination();
                    if (!otmp2Path.isPresent()) {
                        log(queueName, "No available volumes for direct destination. All available destination volumes: " + getAvailableDestinations() + ", In-use by other processes destination volumes: " + inUseDirectDest + ". Exiting queue \"" + p.getName() + "\"");
                        destroyProcessQueue(queueName);
                        return;
                    }
                    tmp2Path = otmp2Path.get();
                    log(queueName, "Reserving volume for direct destination: " + tmp2Path);
                    inUseDirectDest.add(tmp2Path);
                }
            } else {
                tmp2Path = new File(fixVolumePathForWindows(p.getTmp2Drive()), TMP_PATH);
            }
            log(queueName, "Starting process \"" + queueName + "\" " + p.getTmpDrive() + " -> " + p.getTmp2Drive() + ", isTmp2Dest=" + isTmp2Dest + ", tmpPath=" + tmpPath + ", tmp2Path=" + tmp2Path);
            PlotProcess plotProcess = new PlotProcess(queueName, tmpPath, tmp2Path, isTmp2Dest, config.getMemory(), config.getnThreads(), pp -> onCompleteProcess(pp, queueName));
            synchronized (runningProcessQueues) {
                runningProcessQueues.put(queueName, Optional.of(plotProcess));
            }
            plotProcess.startProcess();
        } catch (Exception ex) {
            log(queueName, "createProcess: ERROR: " + ex.getClass() + ": " + ex.getMessage());
            destroyProcessQueue(queueName);
        }
    }

    private void onCompleteProcess(PlotProcess pp, String queueName) {
        try {
            long runtime = System.currentTimeMillis() - pp.getCreateTimestamp();
            log(queueName, "Process complete: \"" + pp.getName() + "\". Runtime: " + Duration.ofMillis(runtime));
            logPlottingStat(pp);
            synchronized (runningProcessQueues) {
                //process finished, but queue is still active
                runningProcessQueues.put(queueName, Optional.empty());
            }
            if (pp.isTmp2Dest()) {
                //plotted directly on destination volume
                synchronized (inUseDirectDest) {
                    inUseDirectDest.remove(pp.getTmp2Path());
                }
            } else {
                //plotted to temp. Initiate move from tmp2 to destination volume
                if (pp.getResultFileName() != null) {
                    boolean delayMove = pp.getTmp2Path().equals(pp.getTmpPath());
                    asyncMover.moveFileAcync(new File(pp.getTmp2Path(), pp.getResultFileName()), queueName,
                            this::getMoveDestinationsList,
                            delayMove ? config.getMoveDelay() : Duration.ZERO);
                } else {
                    log(queueName, "onCompleteProcess: No result file");
                }
            }
            createProcess(queueName);
        } catch (Exception ex) {
            log(queueName, "onCompleteProcess: ERROR: " + ex.getClass() + ": " + ex.getMessage());
            destroyProcessQueue(queueName);
        }
    }

    /**
     * Pick direct destination volume that is not already used, not a network
     * shared volume, preferring one with the lowest fill ratio.
     *
     * @return Optional of destination File object
     */
    private Optional<File> getDirectDestination() {
        synchronized (inUseDirectDest) {
            return getAvailableDestinations().stream()
                    .filter(f -> !inUseDirectDest.contains(f) && !IOUtils.isNetworkDriveCached(f) && !new File(f, NO_DIRECT_FILENAME).exists() && (f.getParentFile() == null || !new File(f.getParentFile(), NO_DIRECT_FILENAME).exists()))
                    .sorted(Comparator.comparing(this::getFillRatio))
                    .findFirst();
        }
    }

    /**
     * Get list of available destinations. List first destination that are not
     * used by direct plotting and ones with the largest free space.
     *
     * @return Collection of destination File objects
     */
    private Collection<File> getMoveDestinationsList() {
        synchronized (inUseDirectDest) {
            return getAvailableDestinations().stream()
                    .sorted(Comparator.comparing((File f) -> inUseDirectDest.contains(f)).thenComparing(Comparator.comparingDouble(this::getFreeSpace).reversed()))
                    .collect(Collectors.toList());
        }
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

    private void onRootChanged(File root, boolean isNew) {
        log(null, (isNew ? "Adding" : "Removing") + " destination volume: "
                + root.getAbsolutePath()
                + (isNew && isNetworkDriveCached(root) ? " (Network shared drive)" : "")
        );
    }

    private Collection<File> getAvailableDestinations() {
        File[] listRoots = File.listRoots();
        IOUtils.updateNetworkDriveCache(listRoots);
        Collection<File> result = Stream.of(listRoots)
                .filter(f -> !new File(f, NO_WRITE_FILENAME).exists())
                .map(f -> new File(f, DESTINATION_PATH))
                .filter(f -> f.exists() && f.isDirectory() && !new File(f, NO_WRITE_FILENAME).exists() && getFreeSpace(f) >= MIN_SPACE)
                .collect(Collectors.toList());
        return result;
    }

    private boolean hasDestinationSpace() {
        return getAvailableDestinations().stream().map(f -> getFreeSpace(f) / MIN_SPACE).reduce(0L, Long::sum) > asyncMover.countMovingProcessesNoDestination();
    }

    private long getFreeSpace(File f) {
        synchronized (inUseDirectDest) {
            return f.getUsableSpace() - getSpaceReservedByDirectDestProcess(f) - getSpaceReservedByMovingProcess(f);
        }
    }

    private long getSpaceReservedByDirectDestProcess(File f) {
        return inUseDirectDest.contains(f) ? MIN_SPACE : 0;
    }

    private long getSpaceReservedByMovingProcess(File f) {
        return asyncMover.getMovingProcessByDestination(f).map(mp -> mp.getFileSize() - mp.getMovedBytes()).orElse(0L);
    }

    private double getFillRatio(File f) {
        double total = f.getTotalSpace();
        return total <= 0 ? 0.5 : ((total - getFreeSpace(f)) / total);
    }

    private void log(String queue, String s) {
        LoggerUtil.log((queue != null ? queue + " " : "") + "ProcessManager(" + VERSION + "): " + s);
    }
}
