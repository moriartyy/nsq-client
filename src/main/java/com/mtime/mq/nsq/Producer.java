package com.mtime.mq.nsq;

import com.mtime.mq.nsq.channel.Channel;
import com.mtime.mq.nsq.channel.ChannelPool;
import com.mtime.mq.nsq.exceptions.NSQException;
import com.mtime.mq.nsq.exceptions.NoConnectionsException;
import com.mtime.mq.nsq.netty.NettyChannelPool;
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
import java.util.concurrent.atomic.AtomicInteger;

public class Producer implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(Producer.class);

    private static final int MAX_CONNECTION_RETRIES = 3;
    /**
     * nsqd servers
     * <p>
     * use topic as key, for different topic may use independent servers.
     * </P>
     */
    private Map<String, ServerAddress[]> servers = new ConcurrentHashMap<>();
    private Map<String, AtomicInteger> topicServerRoundRobinCounts = new ConcurrentHashMap<>();
    private ProducerConfig config;
    private final Map<ServerAddress, ChannelPool> clientPools = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.create("nsqProducerScheduler"));

    public Producer(ProducerConfig config) {
        this.config = config;

        if (this.config.getLookupPeriodMills() != Config.LOOKUP_PERIOD_NEVER) {
            this.scheduler.scheduleAtFixedRate(this::updateServers,
                    config.getLookupPeriodMills(), config.getLookupPeriodMills(), TimeUnit.MILLISECONDS);

        }
    }

    private void updateServers() {
        try {
            this.servers.keySet().forEach(topic -> {
                ServerAddress[] addresses = lookupServers(topic);
                if (addresses.length > 0) {
                    this.servers.put(topic, addresses);
                }
            });
        } catch (Exception e) {
            LOGGER.error("update servers failed", e);
        }
    }

    private ServerAddress[] lookupServers(String topic) {
        return config.getLookup().lookup(topic).toArray(new ServerAddress[0]);
    }

    private Channel acquireChannel(String topic) throws NoConnectionsException {
        int retries = 0;
        while (retries < MAX_CONNECTION_RETRIES) {
            try {
                ServerAddress serverAddress = nextAddress(topic);
                return acquireChannel(serverAddress);
            } catch (Exception ex) {
                LOGGER.error("Acquire channel for topic {} failed", topic, ex);
            }
            retries++;
        }
        throw new NoConnectionsException("Could not acquire a connection from pool after try" + MAX_CONNECTION_RETRIES + " times.");
    }

    private Channel acquireChannel(ServerAddress serverAddress) {
        ChannelPool pool = getPool(serverAddress);
        return pool.acquire();
    }

    private ChannelPool getPool(ServerAddress serverAddress) {
        return clientPools.computeIfAbsent(serverAddress,
                s -> new NettyChannelPool(s, this.config));
    }

    private ServerAddress nextAddress(String topic) {
        ServerAddress[] serversForTopic = servers.computeIfAbsent(topic, this::lookupServers);
        if (serversForTopic.length == 0) {
            throw new IllegalStateException("No server configured for topic " + topic);
        }
        AtomicInteger roundRobinCountForTopic = topicServerRoundRobinCounts.computeIfAbsent(topic, t -> new AtomicInteger());
        return serversForTopic[roundRobinCountForTopic.getAndIncrement() % serversForTopic.length];
    }

    /**
     * produce multiple messages.
     */
    public void produceMulti(String topic, List<byte[]> messages) throws NSQException {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, messages.get(0));
            return;
        }

        sendCommand(topic, Command.multiPublish(topic, messages));
    }

    public void produce(String topic, byte[] message) throws NSQException {
        sendCommand(topic, Command.publish(topic, message));
    }

    public void produceDeferred(String topic, byte[] message, int deferMillis) throws NSQException {
        sendCommand(topic, Command.deferredPublish(topic, message, deferMillis));
    }

    private void sendCommand(String topic, Command command) throws NSQException {
        Channel channel = acquireChannel(topic);
        try {
            channel.sendAndWait(command);
        } catch (Exception e) {
            throw new NSQException("Execute command failed", e);
        } finally {
            getPool(channel.getRemoteServerAddress()).release(channel);
        }
    }

    public void close() {
        clientPools.forEach((s, pool) -> {
            try {
                pool.close();
            } catch (Exception e) {
                LOGGER.error("Caught exception while close pool", e);
            }
        });
        clientPools.clear();
        scheduler.shutdown();
    }
}
