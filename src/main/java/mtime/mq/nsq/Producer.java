package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.channel.ChannelPool;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.exceptions.NoConnectionsException;
import mtime.mq.nsq.netty.NettyChannelPool;
import mtime.mq.nsq.support.CloseableUtils;
import mtime.mq.nsq.support.DaemonThreadFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class Producer implements Closeable {

    private final Map<String /*topic*/, ServerAddress[]> servers = new ConcurrentHashMap<>();
    private final Map<String /*topic*/, AtomicInteger> roundRobinCounts = new ConcurrentHashMap<>();
    private final ProducerConfig config;
    private final Map<ServerAddress, ChannelPool> pools = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.create("nsqProducerScheduler"));
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger pendingCommands = new AtomicInteger(0);
    private final int maxPublishRetries;

    public Producer(ProducerConfig config) {
        validateConfig(config);
        this.config = config;
        this.maxPublishRetries = config.getMaxPublishRetries();

        if (this.config.getLookupPeriodMillis() > 0) {
            this.scheduler.scheduleAtFixedRate(this::updateServers,
                    config.getLookupPeriodMillis(), config.getLookupPeriodMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void validateConfig(ProducerConfig config) {
        Objects.requireNonNull(config.getLookup(), "lookup");
    }

    private void updateServers() {
        try {
            updateServerAddresses();
            closePoolsOfObsoletedServers();
        } catch (Exception e) {
            log.error("update servers failed", e);
        }
    }

    private void closePoolsOfObsoletedServers() {
        Set<ServerAddress> totalAddresses = this.servers.values().stream()
                .flatMap(Arrays::stream).collect(Collectors.toSet());

        Set<ServerAddress> obsoletedAddresses = this.pools.keySet().stream()
                .filter(s -> !totalAddresses.contains(s)).collect(Collectors.toSet());

        obsoletedAddresses.forEach(a -> {
            ChannelPool pool = this.pools.remove(a);
            if (pool != null) {
                CloseableUtils.closeQuietly(pool);
            }
        });
    }

    private void updateServerAddresses() {
        this.servers.keySet().forEach(topic -> {
            ServerAddress[] found = lookupServers(topic);
            if (found.length == 0) {
                return;
            }
            this.servers.put(topic, found);
        });
    }

    private ServerAddress[] lookupServers(String topic) {
        return config.getLookup().lookup(topic).toArray(new ServerAddress[0]);
    }

    private Channel acquire(String topic) throws NoConnectionsException {
        ServerAddress address = nextServer(topic);
        return acquire(address);
    }

    private Channel acquire(ServerAddress server) {
        ChannelPool pool = getPool(server);
        return pool.acquire();
    }

    private ChannelPool getPool(ServerAddress server) {
        return pools.computeIfAbsent(server, a -> new NettyChannelPool(a, this.config));
    }

    private ServerAddress nextServer(String topic) {
        ServerAddress[] serversOfTopic = servers.computeIfAbsent(topic, this::lookupServers);
        if (serversOfTopic.length == 0) {
            throw new IllegalStateException("No server configured for topic " + topic);
        }
        AtomicInteger roundRobinCountForTopic = roundRobinCounts.computeIfAbsent(topic, t -> new AtomicInteger());
        return serversOfTopic[roundRobinCountForTopic.getAndIncrement() % serversOfTopic.length];
    }

    public void publish(String topic, List<byte[]> messages) throws NSQException {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.publish(topic, messages.get(0));
            return;
        }

        publish(topic, Command.multiPublish(topic, messages));
    }

    public void publish(String topic, byte[] message) throws NSQException {
        publish(topic, Command.publish(topic, message));
    }

    public void publish(String topic, byte[] message, int deferMillis) throws NSQException {
        publish(topic, Command.deferredPublish(topic, message, deferMillis));
    }

    private void publish(String topic, Command command) throws NSQException {
        checkRunningState();

        NSQException cause = null;

        int i = 0;
        while (running.get() && i++ < maxPublishRetries) {
            try {
                doPublish(topic, command);
                return;
            } catch (Exception e) {
                cause = NSQException.of(e);
            }
        }

        if (cause != null) {
            throw cause;
        }
    }

    private void doPublish(String topic, Command command) {
        Channel channel = null;
        try {
            countUp();
            channel = acquire(topic);
            Response response = channel.sendAndWait(command);
            if (response.getStatus() == Response.Status.ERROR) {
                throw new NSQException("Publish failed, reason: " + response.getMessage());
            }
        } finally {
            if (channel != null) {
                release(channel);
            }
            countDown();
        }
    }

    private void release(Channel channel) {
        try {
            getPool(channel.getRemoteAddress()).release(channel);
        } catch (Exception e) {
            log.warn("Release channel failed", e);
        }
    }

    private void countDown() {
        pendingCommands.decrementAndGet();
    }

    private void countUp() {
        pendingCommands.incrementAndGet();
    }

    private void checkRunningState() {
        if (!running.get()) {
            throw new NSQException("Producer is closed");
        }
    }

    public void close() {
        running.set(false);
        scheduler.shutdownNow();
        waitPendingCommandsToBeCompleted(TimeUnit.SECONDS.toMillis(5L));
        closePools();
    }

    private void closePools() {
        pools.values().forEach(CloseableUtils::closeQuietly);
    }

    private void waitPendingCommandsToBeCompleted(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (pendingCommands.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
