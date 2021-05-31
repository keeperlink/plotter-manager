/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.IOUtils.fixVolumePathForWindows;
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
        Thread.sleep(2000);
        while (!runningProcessQueues.isEmpty() || asyncMover.getMovingProcessesCount() != 0) {
            Thread.sleep(5000);
            if (ConfigReader.readConfig(configFile, config) || checkChangedAndUpdate(getAvailableDestinations(), cachedDestSet)) {
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
        log(p.getName() + " ProcessManager: Creating process: " + p.getName() + "\t" + p.getTmpDrive() + " -> " + p.getTmp2Drive());
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
            new PlotProcess(p.getName(), tmpPath, tmp2Path, isTmp2Dest, config.getMemory(), config.getnThreads(), pp -> onCompleteProcess(pp, queueName)).startProcess();
        } catch (Exception ex) {
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
            asyncMover.moveFileAcync(new File(pp.getTmp2Path(), pp.getResultFileName()), queueName,
                    () -> getAvailableDestinations().stream().sorted(Comparator.comparing(inUseDirectDest::contains)).collect(Collectors.toList()),
                    delayMove ? config.getMoveDelay() : Duration.ZERO);
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

    private Collection<File> getAvailableDestinations() {
        Collection<File> result = Stream.of(File.listRoots())
                .filter(f -> !new File(f, NO_WRITE_FILENAME).exists())
                .map(f -> new File(f, DESTINATION_PATH))
                .filter(f -> f.exists() && f.isDirectory() && !new File(f, NO_WRITE_FILENAME).exists() && getFreeSpace(f) >= MIN_SPACE)
                .collect(Collectors.toList());
        return result;
    }

    /**
     * Check if newData is differ from oldData. If so, update oldData with
     * newData and return true.
     *
     * @param newData New Data Collection
     * @param oldData Old Data Set
     * @return true if data changed
     */
    private static boolean checkChangedAndUpdate(Collection<File> newData, Set<File> oldData) {
        AtomicBoolean changed = new AtomicBoolean(false);
        synchronized (oldData) {
            newData.forEach(f -> {
                if (!oldData.contains(f)) {
                    log("ProcessManager: Adding destination volume: " + f.getAbsolutePath());
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
}
