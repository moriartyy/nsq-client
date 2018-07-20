package com.github.brainlag.nsq;

import com.github.brainlag.nsq.lookup.Lookup;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @author hongmiao.yu
 */
public class ConsumerTest {
    private String topic = "nsq_client_test";
    private String channel = "nsq_client_test_channel";
    private Lookup lookup = LarkUtils.getLookup();
    private MessageHandler messagePrinter = m -> {
        System.out.println(new String(m.getMessage()));
        m.finished();
    };

    @Test
    public void testConsumeMessage() throws InterruptedException {
        Consumer consumer = new Consumer(lookup, topic, channel, messagePrinter);
        System.out.println("Consumer is ready");
        CountDownLatch keepAlive = new CountDownLatch(1);
        keepAlive.await();
    }
}
