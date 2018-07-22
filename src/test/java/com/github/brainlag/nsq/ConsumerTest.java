package com.github.brainlag.nsq;

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
        Consumer consumer = new Consumer(NsqServers.SUBSCRIBE_LOOKUP, topic, channel, messagePrinter);
        System.out.println("Consumer is ready");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            consumer.printStats(LOGGER);
        }, 1, 1, TimeUnit.SECONDS);

        CountDownLatch keepAlive = new CountDownLatch(1);
        keepAlive.await();
    }
}
