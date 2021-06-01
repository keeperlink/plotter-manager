/*
 * GNU GENERAL PUBLIC LICENSE
 */
package com.sliva.plotter;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * @author Sliva Co
 */
public final class IOUtils {

    public static final boolean IS_WINDOWS_OS = Optional.ofNullable(System.getProperty("os.name")).map(n -> n.toLowerCase().startsWith("windows")).orElse(false);

    public static final int KB = 1024;
    public static final int MB = KB * KB;
    public static final long GB = MB * KB;
    public static final long TB = GB * KB;

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
        if (volumePath != null && volumePath.length() == 1 && IS_WINDOWS_OS) {
            result += ":\\";
        }
        return result;
    }

    public static String getDriveName(String path) {
        String result = path;
        if (path.endsWith(":\\")) {
            result = path.substring(0, path.length() - 1);
        }

        return result;
    }

    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    public static boolean isNetworkDrive(String path) {
        if (path == null || !IS_WINDOWS_OS) {
            return false;
        }
        if (path.startsWith("\\\\")) {
            return true;
        }
        if (path.length() > 1 && path.charAt(1) == ':') {
            String drive = path.substring(0, 2);

            String command = "cmd /c net use " + drive;
            try {
                Process p = Runtime.getRuntime().exec(command);
                InputStream stdout = p.getInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(stdout));
                for (String line = in.readLine(); line != null; line = in.readLine()) {
                    if (line.startsWith("Remote name")) {
                        return true;
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
            }
        }
        return false;
    }

    public static boolean isNetworkDriveCached(File root) {
        return isNetworkDriveCache.computeIfAbsent(root, k -> IOUtils.isNetworkDrive(k.getAbsolutePath()));
    }

    /**
     * Check and remove from cache unmounted drives.
     *
     * @param listRoots
     */
    public static void updateNetworkDriveCache(File[] listRoots) {
        Set<File> rootsSet = new HashSet<>(Arrays.asList(listRoots));
        synchronized (isNetworkDriveCache) {
            for (Iterator<Map.Entry<File, Boolean>> i = isNetworkDriveCache.entrySet().iterator(); i.hasNext();) {
                Map.Entry<File, Boolean> e = i.next();
                if (!rootsSet.contains(e.getKey())) {
                    i.remove();
                }
            }
        }
    }
    private static final Map<File, Boolean> isNetworkDriveCache = new HashMap<>();
}
