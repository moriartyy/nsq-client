package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.channel.ChannelPool;
import mtime.mq.nsq.channel.ChannelPoolFactory;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.lookup.Lookup;
import mtime.mq.nsq.netty.NettyChannelPoolFactory;
import mtime.mq.nsq.support.CloseableUtils;
import mtime.mq.nsq.support.DaemonThreadFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class Producer implements Closeable {

    private final Map<String /*topic*/, List<ServerAddress>> servers = new ConcurrentHashMap<>();
    private final Map<String /*topic*/, AtomicInteger> roundRobins = new ConcurrentHashMap<>();
    private final Map<ServerAddress, ChannelPool> pools = new ConcurrentHashMap<>();
    private final Set<ServerAddress> blacklist = new CopyOnWriteArraySet<>();
    private final Map<ServerAddress, List<ErrorEvent>> serverErrors = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            DaemonThreadFactory.create("nsqProducerScheduler"));
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger pendingCommands = new AtomicInteger(0);
    private final int maxPublishRetries;
    private final Lookup lookup;
    private final ChannelPoolFactory channelPoolFactory;
    private final long haltDurationMillis;
    private final int maxPublishErrors;
    private final long responseTimeout;
    private final long errorTimeout = TimeUnit.MINUTES.toMillis(10);

    public Producer(ProducerConfig config) {
        this(config, createChannelPoolFactory(config));
    }

    private static NettyChannelPoolFactory createChannelPoolFactory(ProducerConfig config) {
        return new NettyChannelPoolFactory(config, config.getConnectionsPerServer());
    }

    public Producer(ProducerConfig config, ChannelPoolFactory channelPoolFactory) {
        validateConfig(config);
        this.lookup = config.getLookup();
        this.maxPublishRetries = config.getMaxPublishRetries();
        this.channelPoolFactory = channelPoolFactory;
        this.haltDurationMillis = config.getHaltDurationMillis();
        this.maxPublishErrors = config.getMaxPublishErrors();
        this.responseTimeout = config.getResponseTimeoutMillis();
        if (config.getLookupPeriodMillis() > 0) {
            this.executor.scheduleAtFixedRate(this::updateServers,
                    config.getLookupPeriodMillis(), config.getLookupPeriodMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void validateConfig(ProducerConfig config) {
        Objects.requireNonNull(config.getLookup(), "lookup");
    }

    private void updateServers() {
        try {
            updateServerAddressesByLookup();
            closePoolsOfObsoletedServers();
        } catch (Exception e) {
            log.error("update servers failed", e);
        }
    }

    private void closePoolsOfObsoletedServers() {
        Set<ServerAddress> totalAddresses = this.servers.values().stream()
                .flatMap(Collection::stream).collect(Collectors.toSet());

        Set<ServerAddress> obsoletedAddresses = this.pools.keySet().stream()
                .filter(s -> !totalAddresses.contains(s)).collect(Collectors.toSet());

        if (!obsoletedAddresses.isEmpty()) {
            // delay close pool
            this.executor.schedule(() -> obsoletedAddresses.forEach(a -> {
                ChannelPool pool = this.pools.remove(a);
                if (pool != null) {
                    CloseableUtils.closeQuietly(pool);
                }
            }), 1, TimeUnit.MINUTES);
        }
    }

    private void updateServerAddressesByLookup() {
        this.servers.keySet().forEach(topic -> {
            List<ServerAddress> found = lookup(topic);
            if (found.isEmpty()) {
                // we reserve cached addresses in case of lookup failed.
                return;
            }
            this.servers.put(topic, found);
        });
    }

    private List<ServerAddress> lookup(String topic) {
        return new CopyOnWriteArrayList<>(this.lookup.lookup(topic));
    }

    private void halt(ServerAddress server) {
        log.info("Try halt server {}", server);

        boolean added = !this.blacklist.add(server);
        if (!added) {
            return;
        }

        this.executor.schedule(() -> {
            // remove error counter
            this.serverErrors.remove(server);

            // close pool
            ChannelPool pool = this.pools.remove(server);
            if (pool != null) {
                CloseableUtils.closeQuietly(pool);
            }

            log.debug("Removed {} from black list", server);
            this.blacklist.remove(server);
        }, haltDurationMillis, TimeUnit.MILLISECONDS);
    }

    private Channel acquire(ServerAddress server) {
        ChannelPool pool = getPool(server);
        return pool.acquire();
    }

    private ChannelPool getPool(ServerAddress server) {
        return pools.computeIfAbsent(server, channelPoolFactory::create);
    }

    private ServerAddress nextServer(String topic) {
        List<ServerAddress> servers = serversOfTopic(topic);

        if (servers.isEmpty()) {
            throw new IllegalStateException("No server available for topic " + topic);
        }

        AtomicInteger roundRobin = roundRobins.computeIfAbsent(topic, t -> new AtomicInteger());
        return servers.get(roundRobin.incrementAndGet() % servers.size());
    }

    private List<ServerAddress> serversOfTopic(String topic) {
        List<ServerAddress> servers = this.servers.computeIfAbsent(topic, this::lookup);

        if (!this.blacklist.isEmpty()) {
            // create a new list, we don't want to remove halt servers from cache
            servers = new ArrayList<>(servers);
            servers.removeAll(this.blacklist);
        }

        return servers;
    }

    public void publish(String topic, List<byte[]> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.publish(topic, messages.get(0));
            return;
        }

        publish(topic, Commands.multiPublish(topic, messages));
    }

    public void publish(String topic, byte[] message) {
        publish(topic, Commands.publish(topic, message));
    }

    public void publish(String topic, byte[] message, int deferMillis) {
        publish(topic, Commands.deferredPublish(topic, message, deferMillis));
    }

    private void publish(String topic, Command command) {
        checkRunningState();

        NSQException exp = null;

        ServerAddress server = null;
        int i = 0;
        while (running.get() && i++ < maxPublishRetries) {
            try {
                server = nextServer(topic);
                doPublish(server, command);
                return;
            } catch (Exception e) {
                log.error("Publish failed {} times, topic={}, server={}", i, topic, server, e);
                if (server != null) {
                    recordError(server);
                }
                exp = NSQException.propagate(e);
            }
        }

        if (exp != null) {
            throw exp;
        }
    }

    private void recordError(ServerAddress server) {
        List<ErrorEvent> errors = this.serverErrors.computeIfAbsent(server, s -> new CopyOnWriteArrayList<>());
        errors.add(new ErrorEvent());

        int count = 0;
        List<ErrorEvent> copy = new ArrayList<>(errors);
        for (ErrorEvent e : copy) {
            if (e.isExpired()) {
                errors.remove(e);
            } else {
                count++;
            }
        }

        if (count >= maxPublishErrors) {
            halt(server);
        }
    }

    private void doPublish(ServerAddress server, Command command) {
        Channel channel = null;
        try {
            countUp();
            channel = acquire(server);
            Response response = channel.send(command, this.responseTimeout);
            if (response.getStatus() == Response.Status.ERROR) {
                throw new NSQException("Received error from remote: " + response.getMessage());
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
        executor.shutdownNow();
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

    class ErrorEvent {
        private long expiration;

        ErrorEvent() {
            this.expiration = System.currentTimeMillis() + errorTimeout;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expiration;
        }
    }
}
