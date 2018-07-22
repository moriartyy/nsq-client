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

    private int threads = Runtime.getRuntime().availableProcessors() * 2;
    private volatile int maxInFlight;
    private volatile int channelRdy;
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
        this.executor = new Executor(threads, this::awakenAll);
        this.maintenanceChannels();
        this.scheduler.scheduleAtFixedRate(this::maintenanceChannels, lookupPeriod, lookupPeriod, TimeUnit.MILLISECONDS);
    }


    protected void printStats(Logger logger) {
        logger.info("maxInFlight: {}, channelRdy: {}, threads: {}, idle: {}",
                this.maxInFlight, this.channelRdy, threads, executor.isIdle());
        this.channels.values().forEach(c -> {
            logger.info("  c.inFlight: {}, c.ready: {}", c.getInFlight(), c.getReady());
        });
    }

    private void updateMaxInFlight() {
        this.maxInFlight = threads + threads * 2;
        this.channelRdy = this.maxInFlight / this.channels.size();
    }

    private void maintenanceChannels() {
        removeDisconnectedChannels();
        updateChannelsByLookup();
        updateMaxInFlight();
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
            return;
        }

        final Channel channel = message.getChannel();

        if (!executor.submit(messageHandler, message)) {
//                LOGGER.trace("Backing off");
            halt(channel);
        } else {
            updateChannelRdy(channel);
        }

//        if (message.getChannel().getLeftMessages() < (messagesPerBatch / 2)) {
//            //request some more!
//            message.getChannel().sendReady(messagesPerBatch);
//        }
    }

    private void updateChannelRdy(Channel channel) {
        if (channel.getReady() < this.channelRdy) {
            channel.sendReady(this.channelRdy);
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

    private Set<ServerAddress> lookup() {
        return lookup.lookup(topic);
    }

    @Override
    public void close() {
        scheduler.shutdown();
        cleanClose();
    }
}
