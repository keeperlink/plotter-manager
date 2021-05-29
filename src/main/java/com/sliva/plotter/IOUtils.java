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
}
