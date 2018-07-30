package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.channel.ChannelFactory;
import mtime.mq.nsq.exceptions.NSQExceptions;
import mtime.mq.nsq.lookup.Lookup;
import mtime.mq.nsq.netty.NettyChannelFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

        ConsumerConfig config = new ConsumerConfig();
        config.setLookup(NsqServers.PRODUCE_LOOKUP);
        config.setLookupPeriodMillis(10000L);
        config.setReconnectIntervalMillis(5000L);
//        config.setMaxInFlight(100);
        ChannelFactory factory = new NettyChannelFactory(config);
        Consumer consumer = new Consumer(config, new ChannelFactory() {

            @Override
            public Channel create(ServerAddress server) {
                return new MockChannel(factory.create(server), server);
            }
        });
        consumer.subscribe(TestConstants.topic, TestConstants.channel, messagePrinter);
        System.out.println("Consumer is ready");
//        printStatus(consumer);

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

    @Slf4j
    static class MockChannel implements Channel {

        private static AtomicInteger instanceCount = new AtomicInteger();
        private final Channel channel;
        private final int index;
        private final ServerAddress serverAddress;
        private volatile boolean connected = true;
        private AtomicInteger counter = new AtomicInteger();

        MockChannel(Channel channel, ServerAddress serverAddress) {
            this.channel = channel;
            this.serverAddress = serverAddress;
            this.index = instanceCount.incrementAndGet();
        }

        @Override
        public int getReadyCount() {
            return 0;
        }

        @Override
        public int getInFlight() {
            return 0;
        }

        @Override
        public void setMessageHandler(MessageHandler messageHandler) {

        }

        @Override
        public ServerAddress getRemoteAddress() {
            return serverAddress;
        }

        @Override
        public void send(Command command) {
            log.debug("sending command: ", command.getLine());
            if (counter.incrementAndGet() > 3) {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public Response sendAndWait(Command command) {
            log.debug("sending command: {} to {}", command.getLine(), serverAddress);
            if (counter.incrementAndGet() > 3) {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                throw NSQExceptions.timeout("Send timeout", serverAddress);
            }
            return Response.ok("ok");
        }

        @Override
        public void close() {
            log.debug("Close channel {}-{}", this.serverAddress, this.index);
            connected = false;
        }

        @Override
        public boolean isConnected() {
            return false;
        }
    }
}
