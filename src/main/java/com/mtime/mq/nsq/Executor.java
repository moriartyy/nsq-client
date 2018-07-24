package com.mtime.mq.nsq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
public interface Executor {
    int threadsCount();

    int queueSize();

    void submit(Runnable task);

    void shutdown();

    class DefaultExecutorImpl implements Executor {
        private static final Logger LOGGER = LogManager.getLogger(DefaultExecutorImpl.class);
        private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors() * 2;
        private final ThreadPoolExecutor executor;
        private final int threads;

        public DefaultExecutorImpl(int threads) {
            this.threads = threads == 0 ? DEFAULT_THREADS : threads;
            this.executor = new ThreadPoolExecutor(this.threads, this.threads,
                    1L, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>());
        }

        @Override
        public int threadsCount() {
            return threads;
        }

        @Override
        public int queueSize() {
            return this.executor.getQueue().size();
        }

        @Override
        public void submit(Runnable task) {
            this.executor.submit(task);
        }

        @Override
        public void shutdown() {
            // waiting messages to be processed
            try {
                shutdownAndWaitTermination();
            } catch (InterruptedException e) {
                LOGGER.warn("Shutdown process was interrupted");
            }
        }

        private void shutdownAndWaitTermination() throws InterruptedException {
            this.executor.shutdown();
            if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                LOGGER.warn("Shutdown process did not completed in time");
                this.executor.getQueue().clear();
                this.executor.shutdownNow();
            }
        }
    }
}
