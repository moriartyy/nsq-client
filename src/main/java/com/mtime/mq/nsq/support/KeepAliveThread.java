package com.mtime.mq.nsq.support;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hongmiao.yu
 */
public class KeepAliveThread {
    private CountDownLatch latch;
    private AtomicBoolean started = new AtomicBoolean(false);
    private Thread thread;

    public static KeepAliveThread createStarted() {
        KeepAliveThread t = new KeepAliveThread();
        t.start();
        return t;
    }

    private KeepAliveThread() {
    }

    public Thread getThread() {
        return this.thread;
    }

    public void join() throws InterruptedException {
        this.thread.join();
    }

    private void start() {
        if (started.compareAndSet(false, true)) {
            thread = new Thread(null, () -> {
                latch = new CountDownLatch(1);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    started.set(false);
                }
            }, "keepAliveThread");
            thread.setDaemon(false);
            thread.start();
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            this.latch.countDown();
        }
    }
}
