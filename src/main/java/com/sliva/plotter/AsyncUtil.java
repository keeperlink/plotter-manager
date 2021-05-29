/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 *
 * @author Sliva Co
 */
public final class AsyncUtil {

    @SuppressWarnings("NestedAssignment")
    public static void asyncReadLines(InputStream input, Charset charset, Consumer<String> consumer) {
        BufferedReader br = new BufferedReader(new InputStreamReader(input, charset));
        CompletableFuture.runAsync(() -> {
            String s;
            try {
                while ((s = br.readLine()) != null) {
                    consumer.accept(s);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                consumer.accept(null);
            }
        });
    }
}
