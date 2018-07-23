package com.mtime.mq.nsq;

import com.mtime.mq.nsq.channel.Channel;
import com.mtime.mq.nsq.exceptions.NSQException;
import com.mtime.mq.nsq.netty.NettyChannel;
import com.mtime.mq.nsq.support.DaemonThreadFactory;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.*;

import static com.mtime.mq.nsq.ConsumerConfig.MAX_IN_FLIGHT_ADAPTIVE;

/**
 * TODO subscribe multi topics
 */
public class Consumer implements Closeable {
    protected static final Logger LOGGER = LogManager.getLogger(Consumer.class);

    private final MessageHandler messageHandler;
    private final ConsumerConfig config;
    private final Map<ServerAddress, Channel> channels = new ConcurrentHashMap<>();

    private volatile int maxInFlight;
    private volatile int channelReadyCount;
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.create("nsqConsumerScheduler"));
    private ThreadPoolExecutor executor;

    public Consumer(ConsumerConfig config, MessageHandler messageHandler) {
        this(config, messageHandler, null);
    }

    public Consumer(ConsumerConfig config, MessageHandler messageHandler, ThreadPoolExecutor executor) {
        validateConfig(config);

        this.config = config;
        this.messageHandler = messageHandler;

        if (executor == null) {
            this.executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(),
                    1L, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>());
        } else {
            this.executor = executor;
        }

        if (config.getMaxInFlight() == MAX_IN_FLIGHT_ADAPTIVE) {
            this.maxInFlight = Math.round(config.getThreads() * 2);
        } else {
            this.maxInFlight = config.getMaxInFlight();
        }

        this.initChannels();

        this.scheduler.scheduleAtFixedRate(this::maintenanceChannels,
                this.config.getLookupPeriodMills(), this.config.getLookupPeriodMills(), TimeUnit.MILLISECONDS);
    }

    private void initChannels() {
        updateChannelsByLookup();
    }

    private void validateConfig(ConsumerConfig config) {
        Objects.requireNonNull(config.getTopic(), "topic");
        Objects.requireNonNull(config.getChannel(), "channel");
        Objects.requireNonNull(config.getLookup(), "lookup");
    }

    private void maintenanceChannels() {
        try {
            removeDisconnectedChannels();
            updateChannelsByLookup();
        } catch (Exception e) {
            LOGGER.error("Maintenance channel failed", e);
        }
    }

    private void updateChannelsReadyCounts() {
        int newRdy = this.maxInFlight / this.channels.size();
        if (newRdy != this.channelReadyCount) {
            this.channelReadyCount = newRdy;
            this.channels.values().forEach(c -> {
                try {
                    c.sendReady(this.channelReadyCount);
                } catch (NSQException e) {
                    closeQuietly(c);
                    LOGGER.error("Exception caught while sending read to channel(address={})", c.getRemoteServerAddress(), e);
                }
            });
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
        removeObsoletedChannels(found);
        connectToNewServers(found);
        updateChannelsReadyCounts();
    }

    private void connectToNewServers(Set<ServerAddress> found) {
        found.forEach(s -> {
            if (!channels.containsKey(s)) {
                try {
                    channels.put(s, createChannel(s));
                } catch (Exception e) {
                    LOGGER.error("Failed to create channel from address {}", s, e);
                }
            }
        });
    }

    private void removeObsoletedChannels(Set<ServerAddress> found) {
        this.channels.forEach((s, c) -> {
            if (!found.contains(s)) {
                closeQuietly(c);
                this.channels.remove(s);
            }
        });
    }

    private void closeQuietly(Channel c) {
        try {
            c.close();
        } catch (Exception e) {
            LOGGER.error("Exception caught while closing channel, address: {}", c.getRemoteServerAddress(), e);
        }
    }

    private Channel createChannel(final ServerAddress serverAddress) {
        Channel channel = NettyChannel.instance(serverAddress, config);
        channel.setMessageHandler(this::processMessage);
        Response response = channel.sendSubscribe(this.config.getTopic(), this.config.getChannel());
        if (response.getStatus() == Response.Status.ERROR) {
            throw new NSQException("Subscribe failed reason: " + response.getMessage());
        }
        return channel;
    }

    private void processMessage(final Message message) {
        if (config.isFastFinish()) {
            message.finished();
        }

        if (messageHandler == null) {
            LOGGER.warn("NO Callback, dropping message: " + message);
            return;
        }

        this.executor.execute(() -> {
            try {
                this.messageHandler.process(message);
            } catch (Exception e) {
                LOGGER.error("Process message failed, id={}, topic={}, channel={}",
                        new String(message.getId()), this.config.getTopic(), this.config.getChannel(), e);
            }
        });
    }

    private void cleanClose() {
        for (final Channel channel : channels.values()) {
            closeQuietly(channel);
        }
    }

    private Set<ServerAddress> lookup() {
        return this.config.getLookup().lookup(this.config.getTopic());
    }

    @Override
    public void close() {
        scheduler.shutdown();
        cleanClose();
    }

    public Status getStatus() {
        Status status = new Status();
        status.maxInFlight = this.maxInFlight;
        status.channelReadyCount = this.channelReadyCount;
        status.threads = this.config.getThreads();
        status.queueSize = this.executor.getQueue().size();
        this.channels.values().forEach(status::AddChannelStatus);
        return status;
    }

    @Getter
    public static class Status {
        private int maxInFlight;
        private int channelReadyCount;
        private int threads;
        private int queueSize;
        private Map<ServerAddress, ChannelStatus> channels = new TreeMap<>();

        void AddChannelStatus(Channel channel) {
            this.channels.put(channel.getRemoteServerAddress(), new ChannelStatus(channel));
        }

        @Getter
        static class ChannelStatus {
            private int inFlight;
            private int ready;
            private boolean connected;

            ChannelStatus(Channel channel) {
                this.inFlight = channel.getInFlight();
                this.ready = channel.getReadyCount();
                this.connected = channel.isConnected();
            }
        }
    }
}
