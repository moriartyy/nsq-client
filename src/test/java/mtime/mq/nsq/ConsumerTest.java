package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.lookup.Lookup;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class ConsumerTest {
    private static MessageHandler messagePrinter = m -> {
//        System.out.println(new String(m.getMessage()));
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException ignored) {
//        }
        m.finished();
    };

    @Test
    public void testConsumeMessage() throws InterruptedException {
        Consumer consumer = createConsumer();
        consumer.subscribe(Constants.topic, Constants.channel, messagePrinter);
        System.out.println("Consumer is ready");
        printStatus(consumer);

        CountDownLatch keepAlive = new CountDownLatch(1);
        keepAlive.await();
    }

    private void printStatus(Consumer consumer) {
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
    }

    private static Consumer createConsumer() {
        ConsumerConfig config = new ConsumerConfig();
        config.setLookup(lookup);
        config.setLookupPeriodMillis(3000L);
//        config.setMaxInFlight(100);

        return new Consumer(config);
    }

    private static int lookupTimes = 0;
    static Lookup lookup = topic -> {
        lookupTimes++;
        if ((lookupTimes >= 2 && lookupTimes < 10) || lookupTimes >= 20 && lookupTimes < 30) {
            List<ServerAddress> ss = new ArrayList<>(NsqServers.PRODUCE_SERVERS);
            ss.remove(0);
            return new HashSet<>(ss);
        } else {
            return NsqServers.PRODUCE_SERVERS;
        }
    };

    public static void main(String[] args) throws InterruptedException {
        new ConsumerTest().testConsumeMessage();
    }
}
