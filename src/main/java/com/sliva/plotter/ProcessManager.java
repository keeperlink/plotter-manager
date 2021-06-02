/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.checkChangedAndUpdate;
import static com.sliva.plotter.IOUtils.fixVolumePathForWindows;
import static com.sliva.plotter.IOUtils.isNetworkDriveCached;
import static com.sliva.plotter.LoggerUtil.getTimestampString;
import static com.sliva.plotter.LoggerUtil.log;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    private static final Duration CHECK_PERIOD = Duration.ofSeconds(5);

    private final File configFile;
    private final Config config = new Config();
    private final AsyncMover asyncMover = new AsyncMover();
    private final Set<File> inUseDirectDest = new HashSet<>();
    private final Set<String> runningProcessQueues = new HashSet<>();
    private final AtomicInteger queueCount = new AtomicInteger();

    public ProcessManager(File configFile) {
        this.configFile = configFile;
    }

    @SuppressWarnings("SleepWhileInLoop")
    public void run() throws InterruptedException {
        log("ProcessManager: Using plotting log file: " + PLOTTING_LOG_FILE.getAbsolutePath());
        log("ProcessManager STARTED: Watching for stop file: " + STOP_FILE.getAbsolutePath());
        Set<File> cachedDestSet = new HashSet<>(getAvailableDestinations());
        ConfigReader.readConfig(configFile, config);
        log("ProcessManager: Available destinations: " + getAvailableDestinations());
        config.getQueueNames().forEach(this::createProcessQueue);
        while (!runningProcessQueues.isEmpty() || asyncMover.getMovingProcessesCount() != 0) {
            Thread.sleep(CHECK_PERIOD.toMillis());
            if (ConfigReader.readConfig(configFile, config) || checkChangedAndUpdate(getAvailableDestinations(), cachedDestSet, this::onRootChanged)) {
                //restart non-running queues on any change in either config file or destination volumes availability
                config.getQueueNames().stream().filter(q -> !isQueueRunning(q))
                        .forEach(this::createProcessQueue);
            }
        }
        log("ProcessManager FINISHED");
    }

    private void createProcessQueue(String queueName) {
        int queueId = queueCount.getAndIncrement();
        log(queueName + " ProcessManager: Creating new process queue \"" + queueName + "\" #" + queueId);
        synchronized (runningProcessQueues) {
            if (runningProcessQueues.contains(queueName)) {
                log(queueName + " ProcessManager: Queue already exists - exiting");
                return;
            }
            runningProcessQueues.add(queueName);
        }
        CompletableFuture.runAsync(() -> {
            Duration delay = Duration.ofMillis(config.getDelayStartQueue().toMillis() * queueId);
            if (delay.toMillis() > 0) {
                log(queueName + " ProcessManager: Delaying queue \"" + queueName + "\" #" + queueId + " for " + delay);
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ex) {
                    log(queueName + " ProcessManager: Interrupted during delay sleep queue \"" + queueName + "\" #" + queueId);
                    destroyProcessQueue(queueName);
                    return;
                }
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

    @SuppressWarnings("UseSpecificCatch")
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
        PlotterParams p = config.getPlotterParams(queueName);
        if (p == null) {
            log(queueName + " ProcessManager: Queue removed from config. Exiting queue \"" + queueName + "\"");
            destroyProcessQueue(queueName);
            return;
        }
        log(queueName + " ProcessManager: Creating process: " + queueName + "\t" + p.getTmpDrive() + " -> " + p.getTmp2Drive());
        try {
            boolean isTmp2Dest = "dest".equals(p.getTmp2Drive());
            File tmpPath = new File(fixVolumePathForWindows(p.getTmpDrive()), TMP_PATH);
            File tmp2Path;
            if (isTmp2Dest) {
                synchronized (inUseDirectDest) {
                    Optional<File> otmp2Path = getDirectDestination();
                    if (!otmp2Path.isPresent()) {
                        log(queueName + " ProcessManager: No available volumes for direct destination. All available destination volumes: " + getAvailableDestinations() + ", In-use by other processes destination volumes: " + inUseDirectDest + ". Exiting queue \"" + p.getName() + "\"");
                        destroyProcessQueue(queueName);
                        return;
                    }
                    tmp2Path = otmp2Path.get();
                    log(queueName + " ProcessManager: Reserving volume for direct destination: " + tmp2Path);
                    inUseDirectDest.add(tmp2Path);
                }
            } else {
                tmp2Path = new File(fixVolumePathForWindows(p.getTmp2Drive()), TMP_PATH);
            }
            log(queueName + " ProcessManager: Starting process \"" + queueName + "\" " + p.getTmpDrive() + " -> " + p.getTmp2Drive() + ", isTmp2Dest=" + isTmp2Dest + ", tmpPath=" + tmpPath + ", tmp2Path=" + tmp2Path);
            new PlotProcess(queueName, tmpPath, tmp2Path, isTmp2Dest, config.getMemory(), config.getnThreads(), pp -> onCompleteProcess(pp, queueName)).startProcess();
        } catch (Exception ex) {
            log(queueName + " ProcessManager: createProcess: ERROR: " + ex.getClass() + ": " + ex.getMessage());
            destroyProcessQueue(queueName);
        }
    }

    private void onCompleteProcess(PlotProcess pp, String queueName) {
        try {
            long runtime = System.currentTimeMillis() - pp.getCreateTimestamp();
            log(queueName + " ProcessManager: Process complete: \"" + pp.getName() + "\". Runtime: " + Duration.ofMillis(runtime));
            logPlottingStat(pp);
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
                    log(queueName + " ProcessManager: onCompleteProcess: No result file");
                }
            }
            createProcess(queueName);
        } catch (Exception ex) {
            log(queueName + " ProcessManager: onCompleteProcess: ERROR: " + ex.getClass() + ": " + ex.getMessage());
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
                    .filter(f -> !inUseDirectDest.contains(f) && !IOUtils.isNetworkDriveCached(f))
                    .sorted(Comparator.comparing(this::getFillRatio))
                    .findFirst();
        }
    }

    /**
     * Get list of available destinations. List first destination that are not
     * used by direct plotting and ones with the lowest fill ratio.
     *
     * @return Collection of destination File objects
     */
    private Collection<File> getMoveDestinationsList() {
        synchronized (inUseDirectDest) {
            return getAvailableDestinations().stream()
                    .sorted(Comparator.comparing((File f) -> inUseDirectDest.contains(f)).thenComparingDouble(this::getFillRatio))
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
        log("ProcessManager: " + (isNew ? "Adding" : "Removing") + " destination volume: "
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

    private long getFreeSpace(File f) {
        synchronized (inUseDirectDest) {
            return f.getUsableSpace() - (inUseDirectDest.contains(f) ? MIN_SPACE : 0);
        }
    }

    private double getFillRatio(File f) {
        double total = f.getTotalSpace();
        return total <= 0 ? 0.5 : ((total - getFreeSpace(f)) / total);
    }
}
