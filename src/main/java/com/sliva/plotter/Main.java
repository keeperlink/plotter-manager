/*
 * GNU GENERAL PUBLIC LICENSE
 */
package com.sliva.plotter;

import java.io.File;

/**
 *
 * @author Sliva Co
 */
public class Main {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: java -jar Plotter.jar <config-file>");
            return;
        }
        File configFile = new File(args[0]);
        if (!configFile.exists()) {
            System.out.println("ERROR: Config file doesn't exist: " + configFile.getAbsolutePath());
            return;
        }
        new ProcessManager(configFile).run();
    }
}
