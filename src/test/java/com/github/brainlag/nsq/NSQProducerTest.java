package com.github.brainlag.nsq;

import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.lookup.DefaultLookup;
import com.github.brainlag.nsq.lookup.Lookup;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NSQProducerTest {
    protected static final Logger LOGGER = LogManager.getLogger(NSQProducerTest.class);

    private Config getSnappyConfig() {
        final Config config = new Config();
        config.setCompression(Config.Compression.SNAPPY);
        return config;
    }

    private Config getDeflateConfig() {
        final Config config = new Config();
        config.setCompression(Config.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    private Config getSslConfig() throws SSLException {
        final Config config = new Config();
        File serverKeyFile = new File(getClass().getResource("/server.pem").getFile());
        File clientKeyFile = new File(getClass().getResource("/client.key").getFile());
        File clientCertFile = new File(getClass().getResource("/client.pem").getFile());
        SslContext ctx = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(serverKeyFile)
                .keyManager(clientCertFile, clientKeyFile).build();
        config.setSslContext(ctx);
        return config;
    }

    private Config getSslAndSnappyConfig() throws SSLException {
        final Config config = getSslConfig();
        config.setCompression(Config.Compression.SNAPPY);
        return config;
    }

    private Config getSslAndDeflateConfig() throws SSLException {
        final Config config = getSslConfig();
        config.setCompression(Config.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    ServerAddress localhost = new ServerAddress("localhost", 4161);
    Set<ServerAddress> serverAddresses = ImmutableSet.of(new ServerAddress("localhost", 4161));

    @Test
    public void testProduceOneMsgSnappy() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Producer producer = new Producer(serverAddresses, getSnappyConfig());
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.close();

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSnappyConfig());
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.close();
    }

    @Test
    public void testProduceOneMsgDeflate() throws NSQException, TimeoutException, InterruptedException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Producer producer = new Producer(serverAddresses, getDeflateConfig());
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.close();

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getDeflateConfig());
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.close();
    }

    @Test
    public void testProduceOneMsgSsl() throws InterruptedException, NSQException, TimeoutException, SSLException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Producer producer = new Producer(serverAddresses, getSslConfig());
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.close();

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSslConfig());
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.close();
    }

    @Test
    public void testProduceOneMsgSslAndSnappy() throws InterruptedException, NSQException, TimeoutException, SSLException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Producer producer = new Producer(serverAddresses, getSslConfig());
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.close();

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSslAndSnappyConfig());
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.close();
    }

    @Test
    public void testProduceOneMsgSslAndDeflat() throws InterruptedException, NSQException, TimeoutException, SSLException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Producer producer = new Producer(serverAddresses, getSslConfig());
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.close();

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSslAndDeflateConfig());
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.close();
    }


    @Test
    public void testProduceMoreMsg() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });

        Producer producer = new Producer(serverAddresses);
        for (int i = 0; i < 1000; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.close();

        while (counter.get() < 1000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 1000);
        consumer.close();
    }

    @Test
    public void testParallelProducer() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });

        Producer producer = new Producer(serverAddresses);
        for (int n = 0; n < 5; n++) {
            new Thread(() -> {
                for (int i = 0; i < 1000; i++) {
                    String msg = randomString();
                    try {
                        producer.produce("test3", msg.getBytes());
                    } catch (NSQException e) {
                        Throwables.propagate(e);
                    }
                }
            }).start();
        }
        while (counter.get() < 5000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 5000);
        producer.close();
        consumer.close();
    }

    @Test
    public void testMultiMessage() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });

        Producer producer = new Producer(serverAddresses);
        List<byte[]> messages = Lists.newArrayList();
        for (int i = 0; i < 50; i++) {
            messages.add(randomString().getBytes());
        }
        producer.produceMulti("test3", messages);
        producer.close();

        while (counter.get() < 50) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 50);
        consumer.close();
    }

    @Test
    public void testBackoff() throws InterruptedException, NSQException, TimeoutException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);

        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
            LOGGER.info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            message.finished();
        });
//        consumer.setThreads(1);
//        consumer.setExecutor(newBackoffThreadExecutor());

        Producer producer = new Producer(serverAddresses);
        for (int i = 0; i < 20; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.close();

        while (counter.get() < 20) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 20);
        consumer.close();
    }

    @Test
    public void testScheduledCallback() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Lookup lookup = new DefaultLookup(serverAddresses);


        Consumer consumer = new Consumer(lookup, "test3", "testconsumer", (message) -> {
        });
        consumer.scheduleRun(() -> counter.incrementAndGet(), 1000, 1000, TimeUnit.MILLISECONDS);

        Thread.sleep(1000);
        assertTrue(counter.get() == 1);
        consumer.close();
    }

    public static ExecutorService newBackoffThreadExecutor() {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1));
    }

    private String randomString() {
        return "Message" + new Date().getTime();
    }
}
