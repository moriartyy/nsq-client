package mtime.mq.nsq;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
public class ProducerTest {

    @Test
    public void testProduceMessage() throws InterruptedException {
        String topic = TestConstants.topic;
        ProducerConfig config = new ProducerConfig();
        config.setLookup(NsqServers.PRODUCE_LOOKUP);
        config.setMaxPublishRetries(3);
        config.setHaltDurationMillis(TimeUnit.SECONDS.toMillis(10));
        config.setMaxPublishErrors(1);
        config.setResponseTimeoutMillis(1000L);
        config.setConnectionTimeoutMillis(1000L);
        Producer producer = new Producer(config);
        Thread t = new Thread(() -> {
            while (true) {
                String message = ("hello " + LocalDateTime.now().toString());
                try {
                    producer.publish(topic, message.getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
//            System.out.println("Sending to " + topic + ": " + message);
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
        while (true) {
            String message = ("hello " + LocalDateTime.now().toString());
            try {
                producer.publish(topic, message.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
//            System.out.println("Sending to " + topic + ": " + message);
            Thread.sleep(1L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }

    public static void main(String[] args) throws InterruptedException {
        new ProducerTest().testProduceMessage();
    }

//    class MockChannelPoolFactory implements ChannelPoolFactory {
//
//        @Override
//        public ChannelPool create(ServerAddress serverAddress) {
//            return new MockChannelPool(serverAddress);
//        }
//    }
//
//    @Slf4j
//    static class MockChannelPool implements ChannelPool {
//        private static AtomicInteger instanceCount = new AtomicInteger();
//        private final ServerAddress serverAddress;
//        private final MockChannel channel;
//        private AtomicInteger counter = new AtomicInteger();
//        private final int index;
//
//        public MockChannelPool(ServerAddress serverAddress) {
//            this.serverAddress = serverAddress;
//            this.channel = new MockChannel(serverAddress);
//            this.index = instanceCount.incrementAndGet();
//            log.debug("Create pool {}-{}", serverAddress, index);
//        }
//
//        @Override
//        public Channel acquire() {
////            int count = counter.incrementAndGet();
////            if (count > 3 && this.index % 2 == 1) {
////                log.debug("Acquire throwing fake exception");
////                throw new NSQException("Fake Exception");
////            }
//            return channel;
//        }
//
//        @Override
//        public void release(Channel channel) {
////            log.debug("Release channel {}", this.serverAddress);
//        }
//
//        @Override
//        public void close() {
//            log.debug("Close pool {}", this.serverAddress);
//            this.channel.close();
//        }
//    }
//
//    @Slf4j
//    static class MockChannel implements Channel {
//
//        private static AtomicInteger instanceCount = new AtomicInteger();
//        private final ServerAddress serverAddress;
//        private final int index;
//        private volatile boolean connected = true;
//        private AtomicInteger counter = new AtomicInteger();
//
//        MockChannel(ServerAddress serverAddress) {
//            this.serverAddress = serverAddress;
//            this.index = instanceCount.incrementAndGet();
//        }
//
//        @Override
//        public int getReadyCount() {
//            return 0;
//        }
//
//        @Override
//        public int getInFlight() {
//            return 0;
//        }
//
//        @Override
//        public void setMessageHandler(MessageHandler messageHandler) {
//
//        }
//
//        @Override
//        public ServerAddress getRemoteAddress() {
//            return serverAddress;
//        }
//
//        @Override
//        public ResponseFuture send(Command command) {
//            log.debug("sending command: {} to {}", command.getLine(), serverAddress);
//            if (counter.incrementAndGet() > 3) {
//                try {
//                    Thread.sleep(5000L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                throw NSQExceptions.timeout("Send timeout", serverAddress);
//            }
//            ResponseFuture f = new ResponseFuture(serverAddress);
//            f.set(Response.ok("ok"));
//            return f;
//        }
//
//        @Override
//        public void close() {
//            log.debug("Close channel {}-{}", this.serverAddress, this.index);
//            connected = false;
//        }
//
//        @Override
//        public boolean isConnected() {
//            return connected;
//        }
//    }

}
