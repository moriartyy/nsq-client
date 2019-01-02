package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.support.ExecutorUtils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class DefaultExecutor implements Executor {
    private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
    private final ThreadPoolExecutor executor;
    private final int threads;

    DefaultExecutor(int threads) {
        this.threads = threads == 0 ? DEFAULT_THREADS : threads;
        this.executor = new ThreadPoolExecutor(this.threads, this.threads,
                1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), new ExecutorThreadFactory());
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

    static class ExecutorThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        ExecutorThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "nsq-executor-pool-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);

            t.setUncaughtExceptionHandler(defaultThreadExceptionHandler);

            return t;
        }
    }

    static Thread.UncaughtExceptionHandler defaultThreadExceptionHandler = (t, e) -> log.error("Error happened", e);
}
