package com.github.brainlag.nsq;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by guohua.cui on 16/4/13.
 */
public class Executor {
    private ThreadPool pool;

    public Executor(int threads, IdleNotifier notifier) {
        threads = Math.min(threads, 32);
        int backlog = Math.max(threads, 10);
        pool = new ThreadPool(threads, backlog, notifier);
    }

    /**
     * 提交消息
     *
     * @param messageHandler
     * @param msg
     * @return true if can accept more messages.
     */
    public synchronized boolean submit(MessageHandler messageHandler, final Message msg) {
        pool.submit(() -> messageHandler.process(msg));
        return pool.isIdle();
    }

    public boolean isIdle() {
        return pool.isIdle();
    }

    public int getQueueSize() {
        return pool.getQueue().size();
    }

    @FunctionalInterface
    interface IdleNotifier {
        void announce();
    }

    static class ThreadPool extends ThreadPoolExecutor {
        private IdleNotifier notifier;
        private int backlog;

        public ThreadPool(int poolSize, int backlog, IdleNotifier notifier) {
            super(poolSize, poolSize, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
            this.backlog = backlog;
            this.notifier = notifier;
        }

        public boolean isIdle() {
            return (this.backlog + this.getCorePoolSize()) > (this.getActiveCount() + this.getQueue().size());
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            if (this.isIdle()) {
                this.notifier.announce();
            }
        }
    }
}
