package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class ConsumerTest {
    private static String topic = "nsq_client_test";
    private static String channel = "nsq_client_test_channel";
    private static MessageHandler messagePrinter = m -> {
//        System.out.println(new String(m.getMessage()));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        m.finished();
    };

    @Test
    public void testConsumeMessage() throws InterruptedException {
        Consumer consumer = createConsumer();
        for (int i = 0; i < 3; i++) {
            consumer.subscribe(topic + i, channel, messagePrinter);
            consumer.subscribe(topic + i, channel + 1, messagePrinter);
        }
        System.out.println("Consumer is ready");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            Consumer.Status status = consumer.getStatus();
            status.getSubscriptions().forEach(subscriptionStatus -> {
                System.out.println("Subscription("
                        + "topic=" + subscriptionStatus.getTopic()
                        + ", channel=" + subscriptionStatus.getChannel()
                        + ", maxInFlight=" + subscriptionStatus.getMaxInFlight()
                        + ", threads=" + subscriptionStatus.getThreads()
                        + ", queueSize=" + subscriptionStatus.getQueueSize()
                        + ")");
                subscriptionStatus.getChannels().forEach(channelStatus -> {
                    System.out.println("    Channel("
                            + "address=" + channelStatus.getRemoteAddress().toString()
                            + ", inFlight=" + channelStatus.getInFlight()
                            + ", ready=" + channelStatus.getReadyCount()
                            + ", isConnected=" + channelStatus.isConnected()
                            + ")");
                });
            });
            System.out.println();
        }, 1, 1, TimeUnit.SECONDS);

        CountDownLatch keepAlive = new CountDownLatch(1);
        keepAlive.await();
    }

    private static Consumer createConsumer() {
        ConsumerConfig config = new ConsumerConfig();
        config.setLookup(NsqServers.SUBSCRIBE_LOOKUP);
//        config.setMaxInFlight(100);

        return new Consumer(config);
    }


}
