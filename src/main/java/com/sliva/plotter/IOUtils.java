/*
 * GNU GENERAL PUBLIC LICENSE
 */
package com.sliva.plotter;

import java.io.File;
import java.util.stream.Stream;

/**
 *
 * @author Sliva Co
 */
public final class IOUtils {

    public static final int KB = 1024;
    public static final int MB = KB * KB;
    public static final long GB = MB * KB;
    public static final long TB = GB * KB;

    private static final boolean isWindowsOS = System.getProperty("os.name") != null && System.getProperty("os.name").startsWith("Windows");

    /**
     * Check if there are files with extension fileExtension are present in the
     * directory.
     *
     * @param directory Directory to lookup files in
     * @param fileExtension File extension to look for
     * @return true if at least one file with provided extension is present
     */
    public static boolean isPresentFileWithExt(File directory, String fileExtension) {
        return Stream.of(directory.listFiles(f -> f.isFile() && f.getName().toLowerCase().endsWith("." + fileExtension.toLowerCase()))).findAny().isPresent();
    }

    /**
     * Delete a;; files from directory with names ending specified in
     * extToDelete (i.e. ".tmp")
     *
     * @param directory Directory where files will be deleted
     * @param extToDelete file ending to delete, i.e. ".tmp"
     */
    public static void deleteTempFiles(File directory, String extToDelete) {
        Stream.of(directory.listFiles(f -> f.isFile() && f.getName().endsWith(extToDelete))).forEach(f -> f.delete());
    }

    /**
     * If volume path is a single character and the system is Windows, then
     * convert it to Drive root path. I.e. "C" ==> "C:\".
     *
     * @param volumePath Volume path string
     * @return fixed path
     */
    public static String fixVolumePathForWindows(String volumePath) {
        String result = volumePath;
        if (volumePath != null && volumePath.length() == 1 && isWindowsOS) {
            result += ":\\";
        }
        return result;
    }
}
