package mtime.mq.nsq;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.channel.ChannelFactory;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.lookup.Lookup;
import mtime.mq.nsq.netty.NettyChannelFactory;
import mtime.mq.nsq.support.CloseableUtils;
import mtime.mq.nsq.support.DaemonThreadFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class Consumer implements Closeable {

    private final ConsumerConfig config;
    private final Map<Subscription, List<Channel>> subscriptions = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            DaemonThreadFactory.create("nsqConsumerScheduler"));
    private final Lookup lookup;
    private final ChannelFactory channelFactory;

    public Consumer(ConsumerConfig config) {
        this(config, new NettyChannelFactory(config));
    }

    public Consumer(ConsumerConfig config, ChannelFactory channelFactory) {
        validateConfig(config);
        this.config = config;
        this.lookup = config.getLookup();
        this.channelFactory = channelFactory;
        if (this.config.getLookupPeriodMillis() > 0) {
            this.scheduler.scheduleAtFixedRate(this::updateSubscriptions,
                    this.config.getLookupPeriodMillis(), this.config.getLookupPeriodMillis(), TimeUnit.MILLISECONDS);
        }
        this.scheduler.scheduleAtFixedRate(this::reconnectToDisconnectedServers, 1, 1, TimeUnit.MINUTES);
    }

    private Executor newExecutor(int threads) {
        return new Executor.DefaultImpl(threads);
    }

    public void subscribe(String topic, String channel, MessageHandler messageHandler) {
        subscribe(topic, channel, messageHandler, newExecutor(0), 0);
    }

    public void subscribe(String topic, String channel, MessageHandler messageHandler, int numberOfThreads) {
        subscribe(topic, channel, messageHandler, newExecutor(numberOfThreads), 0);
    }

    public void subscribe(String topic, String channel, MessageHandler messageHandler, Executor executor, int maxInFlight) {
        Subscription subscription = Subscription.builder()
                .topic(topic)
                .channel(channel)
                .handler(messageHandler)
                .executor(executor)
                .maxInFlight(maxInFlight)
                .build();

        if (subscriptions.containsKey(subscription)) {
            throw NSQException.instance("Subscription(topic=" + topic + ", channel=" + channel + ") already exist");
        }

        this.initSubscription(subscription);
    }

    private void initSubscription(Subscription subscription) {
        updateSubscription(
                subscription, subscriptions.computeIfAbsent(subscription, s -> new CopyOnWriteArrayList<>()));
    }

    private void validateConfig(ConsumerConfig config) {
        Objects.requireNonNull(config.getLookup(), "lookup");
    }

    private void updateSubscriptions() {
        subscriptions.forEach((subscription, channels) -> {
            try {
                updateSubscription(subscription, channels);
            } catch (Exception e) {
                log.error("Exception caught while maintenance subscription(topic={}, channel={})",
                        subscription.getTopic(), subscription.getChannel(), e);
            }
        });
    }

    private void updateReadyCountForChannels(int maxInFlight, List<Channel> channels) {
        if (channels.isEmpty()) {
            return;
        }

        final int newReadyCount = maxInFlight / channels.size();
        channels.forEach(c -> {
            if (c.getReadyCount() != newReadyCount) {
                try {
                    c.sendReady(newReadyCount);
                } catch (NSQException e) {
                    CloseableUtils.closeQuietly(c);
                    log.error("Exception caught while sending read to channel(address={})", c.getRemoteAddress(), e);
                }
            }
        });
    }

    private void updateSubscription(Subscription subscription, List<Channel> channels) {

        Set<ServerAddress> found = this.lookup.lookup(subscription.getTopic());

        if (found.isEmpty()) {
            log.error("No servers found for topic '{}'", subscription.getTopic());
        } else {
            disconnectAndRemoveObsoletedServers(found, channels);
            connectToNewlyDiscoveredServers(subscription, found, channels);
        }

        updateReadyCountForChannels(subscription.getMaxInFlight(), channels);
    }

    private void reconnectToDisconnectedServers() {
        this.subscriptions.forEach((subscription, channels) -> {
            List<ServerAddress> disconnectedServers = removeDisconnectedServers(channels);
            disconnectedServers.forEach(s -> tryCreateChannel(subscription, s, channels));
        });
    }

    private List<ServerAddress> removeDisconnectedServers(List<Channel> channels) {
        List<Channel> disconnectedChannels = channels.stream()
                .filter(c -> !c.isConnected()).collect(Collectors.toList());

        channels.removeAll(disconnectedChannels);

        return disconnectedChannels.stream().map(Channel::getRemoteAddress).collect(Collectors.toList());
    }

    private void connectToNewlyDiscoveredServers(Subscription subscription, Set<ServerAddress> servers, List<Channel> channels) {
        Set<ServerAddress> currentServers = channels.stream()
                .map(Channel::getRemoteAddress).collect(Collectors.toSet());

        Set<ServerAddress> newServers = servers.stream()
                .filter(s -> !currentServers.contains(s)).collect(Collectors.toSet());

        newServers.forEach(s -> tryCreateChannel(subscription, s, channels));
    }

    private boolean tryCreateChannel(Subscription subscription, ServerAddress server, List<Channel> channels) {
        try {
            channels.add(createSubscriptionChannel(subscription, server));
            return true;
        } catch (Exception e) {
            log.error("Failed to open channel from address {}", server, e);
        }
        return false;
    }

    private void disconnectAndRemoveObsoletedServers(Set<ServerAddress> servers, List<Channel> channels) {
        List<Channel> obsoletedChannels = channels.stream()
                .filter(c -> !servers.contains(c.getRemoteAddress())).collect(Collectors.toList());

        obsoletedChannels.forEach(this::closeChannel);

        channels.removeAll(obsoletedChannels);
    }

    private Channel createSubscriptionChannel(Subscription subscription, final ServerAddress address) {
        Channel channel = channelFactory.create(address);
        channel.setMessageHandler(new ConsumerMessageHandler(subscription));
        Response response = channel.sendSubscribe(subscription.getTopic(), subscription.getChannel());
        if (response.getStatus() == Response.Status.ERROR) {
            throw NSQException.instance("Subscribe failed reason: " + response.getMessage());
        }
        return channel;
    }

    private void closeSubscriptions() {
        this.subscriptions.forEach((subscription, channels) -> {
            channels.forEach(this::closeChannel);
            subscription.getExecutor().shutdown();
        });
    }

    private void closeChannel(Channel channel) {
        log.debug("Closing channel with remote address {}", channel.getRemoteAddress());
        sendClose(channel);
        waitInFlightMessageToBeProcessed(channel);
        CloseableUtils.closeQuietly(channel);
        log.debug("Channel with remote address {} closed", channel.getRemoteAddress());
    }

    private void waitInFlightMessageToBeProcessed(Channel channel) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (channel.getInFlight() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void sendClose(Channel channel) {
        try {
            Response response = channel.sendClose();
            if (response.getStatus() == Response.Status.ERROR) {
                log.error("Clean close failed, reason: {}", response.getMessage());
            }
        } catch (Exception e) {
            log.error("Send close to channel caught exception", e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        closeSubscriptions();
    }

    public Status getStatus() {
        Status status = new Status();
        this.subscriptions.forEach(status::addSubscriptionStatus);
        return status;
    }

    @Getter
    public static class Status {

        private List<SubscriptionStatus> subscriptions = new ArrayList<>();

        void addSubscriptionStatus(Subscription subscription, List<Channel> channels) {
            this.subscriptions.add(new SubscriptionStatus(subscription, channels));
        }

        @Getter
        public static class SubscriptionStatus {
            private int maxInFlight;
            private int threads;
            private int queueSize;
            private String topic;
            private String channel;
            private List<ChannelStatus> channels = new ArrayList<>();

            SubscriptionStatus(Subscription subscription, List<Channel> channels) {
                this.maxInFlight = subscription.maxInFlight;
                this.threads = subscription.getExecutor().threadsCount();
                this.queueSize = subscription.getExecutor().queueSize();
                this.topic = subscription.getTopic();
                this.channel = subscription.getChannel();
                channels.forEach(this::addChannelStatus);
            }

            void addChannelStatus(Channel channel) {
                this.channels.add(new ChannelStatus(channel));
            }
        }

        @Getter
        public static class ChannelStatus {
            private int inFlight;
            private int readyCount;
            private boolean connected;
            private ServerAddress remoteAddress;

            ChannelStatus(Channel channel) {
                this.inFlight = channel.getInFlight();
                this.readyCount = channel.getReadyCount();
                this.connected = channel.isConnected();
                this.remoteAddress = channel.getRemoteAddress();
            }
        }
    }

    class ConsumerMessageHandler implements MessageHandler {

        private final MessageHandler handler;
        private final Executor executor;
        private final String topic;
        private final String channel;

        ConsumerMessageHandler(Subscription subscription) {
            this.handler = subscription.getHandler();
            this.executor = subscription.getExecutor();
            this.topic = subscription.getTopic();
            this.channel = subscription.getChannel();
        }

        @Override
        public void process(Message message) {
            this.executor.submit(() -> {
                try {
                    this.handler.process(message);
                } catch (Exception e) {
                    log.error("Process message failed, id={}, topic={}, channel={}, server={}",
                            new String(message.getId()), this.topic, this.channel, message.getChannel(), e);
                }
            });
        }
    }

    @Getter
    @Setter
    @Builder
    @EqualsAndHashCode(of = {"topic", "channel"})
    private static class Subscription {
        private String topic;
        private String channel;
        private MessageHandler handler;
        private Executor executor;
        private int maxInFlight;

        Subscription(String topic, String channel, MessageHandler handler, Executor executor, int maxInFlight) {
            Objects.requireNonNull(handler, "MessageHandler can not be null");

            this.topic = topic;
            this.channel = channel;
            this.handler = handler;
            this.executor = executor;
            this.maxInFlight = maxInFlight == 0 ? this.executor.threadsCount() * 2 : maxInFlight;
        }
    }
}
