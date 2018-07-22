package com.github.brainlag.nsq;

import com.github.brainlag.nsq.channel.Channel;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.lookup.Lookup;
import com.github.brainlag.nsq.netty.NettyChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Consumer implements Closeable {
    protected static final Logger LOGGER = LogManager.getLogger(Consumer.class);

    private final Lookup lookup;
    private final String topic;
    private final String channel;
    private final MessageHandler messageHandler;
    private final Config config;
    //    private volatile long nextTimeout = 0;
    private final Map<ServerAddress, Channel> channels = new ConcurrentHashMap<>();
    private Set<Channel> haltedChannels = Collections.synchronizedSet(new HashSet<>());
    private final AtomicLong totalMessages = new AtomicLong(0L);

    private int threads = 8;
    private int messagesPerBatch;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private Executor executor;

    public Consumer(final Lookup lookup, final String topic, final String channel, final MessageHandler messageHandler) {
        this(lookup, topic, channel, messageHandler, new Config());
    }

    public Consumer(final Lookup lookup, final String topic, final String channel, final MessageHandler messageHandler,
                    final Config config) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config;
        this.messageHandler = messageHandler;
        this.messagesPerBatch = config.getMaxInFlight().orElse(200);
        this.executor = new Executor(threads, this::awakenAll);
        this.maintenanceChannels();
        this.scheduler.scheduleAtFixedRate(this::maintenanceChannels, lookupPeriod, lookupPeriod, TimeUnit.MILLISECONDS);
    }

    private void maintenanceChannels() {
        removeDisconnectedChannels();
        updateChannelsByLookup();

        // 防止连接停摆的补漏机制
        if (executor.isIdle()) {
            try {
                int threshold = Math.min(messagesPerBatch, 5);
                channels.forEach((server, channel) -> {
                    if (channel.getLeftMessages() < threshold) {
                        channel.sendReady(threshold);
                    }
                });
            } catch (Exception e) {
                LOGGER.warn("recover from idle failed", e);
            }
        }
    }

    private void removeDisconnectedChannels() {
        this.channels.values().removeIf(c -> !c.isConnected());
    }

    private void updateChannelsByLookup() {
        Set<ServerAddress> found = lookup();

        if (found.isEmpty()) {
            return;
        }

        this.channels.forEach((s, c) -> {
            if (!found.contains(s)) {
                c.close();
                this.channels.remove(s);
            }
        });

        found.forEach(s -> this.channels.computeIfAbsent(s, this::createChannel));
    }

    public int getQueueSize() {
        return this.executor.getQueueSize();
    }

    private Channel createChannel(final ServerAddress serverAddress) {
        try {
            Channel channel = NettyChannel.instance(serverAddress, config);
            channel.setMessageHandler(this::processMessage);
            Response response = channel.sendSubscribe(this.topic, this.channel);
            if (response.getStatus() == Response.Status.ERROR) {
                throw new NSQException("Subscribe failed reason: " + response.getMessage());
            }
            channel.sendReady(1);
            return channel;
        } catch (final Exception e) {
            LOGGER.warn("Could not create connection to server {}", serverAddress.toString(), e);
            return null;
        }
    }

    private void processMessage(final Message message) {
        if (config.isFastFinish()) {
            message.finished();
        }

        if (messageHandler == null) {
            LOGGER.warn("NO Callback, dropping message: " + message);
        } else {
            if (!executor.submit(messageHandler, message)) {
//                LOGGER.trace("Backing off");
                halt(message.getChannel());
                return;
            }
        }

        if (message.getChannel().getLeftMessages() < (messagesPerBatch / 2)) {
            //request some more!
            message.getChannel().sendReady(messagesPerBatch);
        }
    }

    private void halt(Channel channel) {
        if (haltedChannels.add(channel)) {
            LOGGER.trace("Backing off, halt connection: " + channel.getRemoteServerAddress());
            channel.sendReady(0);
        }
    }

    private synchronized void awakenAll() {
        if (haltedChannels.size() > 0) {
            LOGGER.trace("Awaken halt connections");
            try {
                haltedChannels.forEach(channel -> channel.sendReady(1));
            } finally {
                haltedChannels.clear();
            }
        }
    }

    private void cleanClose() {
        for (final Channel channel : channels.values()) {
            try {
                channel.close();
            } catch (Exception e) {
                LOGGER.warn("No clean disconnect {}", channel.getRemoteServerAddress(), e);
            }
        }
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    private Set<ServerAddress> lookup() {
        return lookup.lookup(topic);
    }

    @Override
    public void close() {
        scheduler.shutdown();
        cleanClose();
    }
}
