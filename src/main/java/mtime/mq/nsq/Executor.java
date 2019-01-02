package mtime.mq.nsq;

/**
 * @author hongmiao.yu
 */
public interface Executor {

    int threadsCount();

    int queueSize();

    void submit(Runnable task);

    void shutdown();



}
