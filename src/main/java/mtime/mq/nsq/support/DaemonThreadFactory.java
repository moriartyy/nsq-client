package mtime.mq.nsq.support;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
public class DaemonThreadFactory {

    public static ThreadFactory create(String name) {
        return new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(null, r);
                t.setDaemon(true);
                t.setName(name + "#" + this.counter.incrementAndGet());
                return t;
            }
        };
    }
}
