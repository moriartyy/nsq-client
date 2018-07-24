package com.mtime.mq.nsq;

import com.mtime.mq.nsq.channel.Channel;
import com.mtime.mq.nsq.channel.ChannelPool;
import com.mtime.mq.nsq.exceptions.NSQException;
import com.mtime.mq.nsq.exceptions.NoConnectionsException;
import com.mtime.mq.nsq.netty.NettyChannelPool;
import com.mtime.mq.nsq.support.CloseableUtils;
import com.mtime.mq.nsq.support.DaemonThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(Producer.class);

    private static final int MAX_CONNECTION_RETRIES = 3;

    private final Map<String /*topic*/, ServerAddress[]> addresses = new ConcurrentHashMap<>();
    private final Map<String /*topic*/, AtomicInteger> roundRobinCounts = new ConcurrentHashMap<>();
    private final ProducerConfig config;
    private final Map<ServerAddress, ChannelPool> clientPools = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.create("nsqProducerScheduler"));
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger pendingCommands = new AtomicInteger(0);

    public Producer(ProducerConfig config) {
        this.config = config;

        if (this.config.getLookupPeriodMills() != Config.LOOKUP_PERIOD_NEVER) {
            this.scheduler.scheduleAtFixedRate(this::updateServers,
                    config.getLookupPeriodMills(), config.getLookupPeriodMills(), TimeUnit.MILLISECONDS);

        }
    }

    private void updateServers() {
        try {
            this.addresses.keySet().forEach(topic -> {
                ServerAddress[] addresses = lookupServerAddresses(topic);
                if (addresses.length > 0) {
                    this.addresses.put(topic, addresses);
                }
            });
        } catch (Exception e) {
            LOGGER.error("update servers failed", e);
        }
    }

    private ServerAddress[] lookupServerAddresses(String topic) {
        return config.getLookup().lookup(topic).toArray(new ServerAddress[0]);
    }

    private Channel acquireChannel(String topic) throws NoConnectionsException {
        int retries = 0;
        while (running.get() && retries < MAX_CONNECTION_RETRIES) {
            try {
                ServerAddress address = nextAddress(topic);
                return acquireChannel(address);
            } catch (Exception ex) {
                LOGGER.error("Acquire channel for topic {} failed", topic, ex);
            }
            retries++;
        }
        throw new NoConnectionsException("Could not acquire a connection from pool after try " + MAX_CONNECTION_RETRIES + " times.");
    }

    private Channel acquireChannel(ServerAddress address) {
        ChannelPool pool = getPool(address);
        return pool.acquire();
    }

    private ChannelPool getPool(ServerAddress address) {
        return clientPools.computeIfAbsent(address, a -> new NettyChannelPool(a, this.config));
    }

    private ServerAddress nextAddress(String topic) {
        ServerAddress[] addressesOfTopic = addresses.computeIfAbsent(topic, this::lookupServerAddresses);
        if (addressesOfTopic.length == 0) {
            throw new IllegalStateException("No server configured for topic " + topic);
        }
        AtomicInteger roundRobinCountForTopic = roundRobinCounts.computeIfAbsent(topic, t -> new AtomicInteger());
        return addressesOfTopic[roundRobinCountForTopic.getAndIncrement() % addressesOfTopic.length];
    }

    public void multiPublish(String topic, List<byte[]> messages) throws NSQException {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.publish(topic, messages.get(0));
            return;
        }

        sendCommand(topic, Command.multiPublish(topic, messages));
    }

    public void publish(String topic, byte[] message) throws NSQException {
        sendCommand(topic, Command.publish(topic, message));
    }

    public void deferredPublish(String topic, byte[] message, int deferMillis) throws NSQException {
        sendCommand(topic, Command.deferredPublish(topic, message, deferMillis));
    }

    private void sendCommand(String topic, Command command) throws NSQException {
        checkRunningState();
        pendingCommands.incrementAndGet();
        Channel channel = acquireChannel(topic);
        try {
            Response response = channel.sendAndWait(command);
            if (response.getStatus() == Response.Status.ERROR) {
                throw new NSQException("Publish failed, reason: " + response.getMessage());
            }
        } finally {
            pendingCommands.decrementAndGet();
            getPool(channel.getRemoteAddress()).release(channel);
        }
    }

    private void checkRunningState() {
        if (!running.get()) {
            throw new NSQException("Producer is closed");
        }
    }

    public void close() {
        running.set(false);
        scheduler.shutdownNow();
        waitPendingCommandsToBeCompleted();
        closePools();
    }

    private void closePools() {
        clientPools.values().forEach(CloseableUtils::closeQuietly);
    }

    private void waitPendingCommandsToBeCompleted() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5L);
        while (pendingCommands.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
