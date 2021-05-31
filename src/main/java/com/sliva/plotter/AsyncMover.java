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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 *
 * @author Sliva Co
 */
public class AsyncMover {

    private static final int COPY_BUFFER_SIZE = 10 * MB;
    private final AtomicInteger movingProcessesCount = new AtomicInteger();
    private final Set<File> inUseMoveDest = new HashSet<>();

    public int getMovingProcessesCount() {
        return movingProcessesCount.get();
    }

    public void moveFileAcync(File srcFile, String queueName, Supplier<Collection<File>> availableDestinations, Duration delayMove) {
        movingProcessesCount.incrementAndGet();
        CompletableFuture.runAsync(() -> moveFile(srcFile, queueName, availableDestinations, delayMove));
    }

    @SuppressWarnings({"SleepWhileInLoop", "SleepWhileHoldingLock"})
    private void moveFile(File srcFile, String queueName, Supplier<Collection<File>> availableDestinations, Duration delayMove) {
        try {
            if (delayMove.isZero()) {
                log(queueName + " ProcessManager: Delaying move for " + delayMove + ". File: " + srcFile.getAbsolutePath());
                Thread.sleep(delayMove.toMillis());
            }
            File dest;
            synchronized (inUseMoveDest) {
                for (;;) {
                    //find destination with enough space that is not currently used by another move process and prefer one that is not used as direct destination (temp2=dest)
                    Optional<File> odest = availableDestinations.get().stream().filter(f -> !inUseMoveDest.contains(f)).findFirst();
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

}
