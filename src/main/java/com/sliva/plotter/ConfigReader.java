/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import static com.sliva.plotter.LoggerUtil.log;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 *
 * @author Sliva Co
 */
public final class ConfigReader {

    /**
     * Read configuration file.
     *
     * @param file Config file
     * @param config Config to be updated
     * @return true if config has been changed since last read
     */
    public static boolean readConfig(File file, Config config) {
        boolean changed = false;
        Collection<PlotterParams> ppFromConfig = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            synchronized (config.getPlotterParamsMap()) {
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
                                PlotterParams ppOld = config.getPlotterParamsMap().get(name);
                                if (ppOld == null || !ppOld.equals(pp)) {
                                    config.getPlotterParamsMap().put(name, pp);
                                    changed = true;
                                    log("ProcessManager: readConfig: " + name + "\t " + tmpDrive + " -> " + tmp2Drive);
                                }
                                ppFromConfig.add(pp);
                            }
                        }
                    } else if (s.startsWith("memory=")) {
                        int memory2 = Integer.parseInt(s.split("=")[1].trim());
                        if (memory2 != config.getMemory()) {
                            config.setMemory(memory2);
                            changed = true;
                            log("ProcessManager: readConfig: memory=" + memory2);
                        }
                    } else if (s.startsWith("threads=")) {
                        int nThreads2 = Integer.parseInt(s.split("=")[1].trim());
                        if (nThreads2 != config.getnThreads()) {
                            config.setnThreads(nThreads2);
                            changed = true;
                            log("ProcessManager: eadConfig: nThreads=" + nThreads2);
                        }
                    } else if (s.startsWith("delay=")) {
                        Duration delayStartQueue2 = Duration.ofMinutes(Integer.parseInt(s.split("=")[1].trim()));
                        if (!delayStartQueue2.equals(config.getDelayStartQueue())) {
                            config.setDelayStartQueue(delayStartQueue2);
                            changed = true;
                            log("ProcessManager: readConfig: delayStartQueue=" + delayStartQueue2);
                        }
                    } else if (s.startsWith("move-delay=")) {
                        Duration moveDelay2 = Duration.ofMinutes(Integer.parseInt(s.split("=")[1].trim()));
                        if (!moveDelay2.equals(config.getMoveDelay())) {
                            config.setMoveDelay(moveDelay2);
                            changed = true;
                            log("ProcessManager: readConfig: moveDelay=" + moveDelay2);
                        }
                    }
                }
                for (Iterator<Map.Entry<String, PlotterParams>> i = config.getPlotterParamsMap().entrySet().iterator(); i.hasNext();) {
                    Map.Entry<String, PlotterParams> e = i.next();
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
}
