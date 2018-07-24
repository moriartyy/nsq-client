package com.mtime.mq.nsq.support;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
public class ExecutorUtils {

    public static void shutdownGracefully(ExecutorService executorService) {
        shutdownGracefully(executorService, TimeUnit.SECONDS.toMillis(5));
    }

    public static void shutdownGracefully(ExecutorService executorService, long waitMills) {
        executorService.shutdown();
        try {
            executorService.awaitTermination(waitMills, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }

        if (!executorService.isTerminated()) {
            executorService.shutdownNow();
        }
    }
}
