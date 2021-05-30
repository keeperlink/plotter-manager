/*
 * GNU GENERAL PUBLIC LICENSE
 */
package com.sliva.plotter;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Sliva Co
 */
public final class LoggerUtil {

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static void log(String message) {
        System.out.println(getTimestampString() + " " + message);
    }

    public static String format(long value) {
        return NumberFormat.getIntegerInstance().format(value);
    }

    public static String getTimestampString() {
        return getTimestampString(new Date());
    }

    public static String getTimestampString(Date d) {
        return new SimpleDateFormat(DATE_FORMAT).format(d);
    }
}
