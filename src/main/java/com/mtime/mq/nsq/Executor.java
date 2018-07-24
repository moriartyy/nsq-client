package com.mtime.mq.nsq;

import com.mtime.mq.nsq.support.ExecutorUtils;

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
            ExecutorUtils.shutdownGracefully(this.executor);
        }

    }
}
