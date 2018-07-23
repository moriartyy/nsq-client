package com.mtime.mq.nsq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
public class ConsumerTest {
    private static final Logger LOGGER = LogManager.getLogger(ConsumerTest.class);
    private String topic = "nsq_client_test";
    private String channel = "nsq_client_test_channel";
    private MessageHandler messagePrinter = m -> {
//        System.out.println(new String(m.getMessage()));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        m.finished();
    };

    @Test
    public void testConsumeMessage() throws InterruptedException {
        ConsumerConfig config = new ConsumerConfig();
        config.setLookup(NsqServers.SUBSCRIBE_LOOKUP);
        config.setTopic(topic);
        config.setChannel(channel);
//        config.setMaxInFlight(100);

        Consumer consumer = new Consumer(config, messagePrinter);
        System.out.println("Consumer is ready");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            Consumer.Status status = consumer.getStatus();
            System.out.println("Consumer("
                    + "maxInFlight=" + status.getMaxInFlight()
                    + ", channelRdy=" + status.getChannelReadyCount()
                    + ", threads=" + status.getThreads()
                    + ", queueSize=" + status.getQueueSize()
                    + ")");
            status.getChannels().forEach((a, cs) -> {
                System.out.println("    Channel("
                        + "address=" + a.toString()
                        + ", inFlight=" + cs.getInFlight()
                        + ", ready=" + cs.getReady()
                        + ")");
            });
            System.out.println();
        }, 1, 1, TimeUnit.SECONDS);

        CountDownLatch keepAlive = new CountDownLatch(1);
        keepAlive.await();
    }
}
