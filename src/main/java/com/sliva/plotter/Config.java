/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author Sliva Co
 */
public class Config {

    private int memory = 3500;
    private int nThreads = 4;
    private Duration delayStartQueue = Duration.ofMinutes(60);
    private Duration moveDelay = Duration.ofMinutes(30);
    private final Map<String, PlotterParams> plotterParamsMap = new HashMap<>();

    public Config() {
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getnThreads() {
        return nThreads;
    }

    public void setnThreads(int nThreads) {
        this.nThreads = nThreads;
    }

    public Duration getDelayStartQueue() {
        return delayStartQueue;
    }

    public void setDelayStartQueue(Duration delayStartQueue) {
        this.delayStartQueue = delayStartQueue;
    }

    public Duration getMoveDelay() {
        return moveDelay;
    }

    public void setMoveDelay(Duration moveDelay) {
        this.moveDelay = moveDelay;
    }

    public Map<String, PlotterParams> getPlotterParamsMap() {
        return plotterParamsMap;
    }

    public Collection<String> getQueueNames() {
        synchronized (plotterParamsMap) {
            return new HashSet<>(plotterParamsMap.keySet());
        }
    }

    public PlotterParams getPlotterParams(String queueName) {
        synchronized (plotterParamsMap) {
            return plotterParamsMap.get(queueName);
        }
    }

}
