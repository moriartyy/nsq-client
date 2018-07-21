package com.github.brainlag.nsq;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author hongmiao.yu
 */
public class ConsumerTest {
    private String topic = "nsq_client_test";
    private String channel = "nsq_client_test_channel";
    private MessageHandler messagePrinter = m -> {
        System.out.println(new String(m.getMessage()));
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
        CountDownLatch keepAlive = new CountDownLatch(1);
        keepAlive.await();
    }
}
