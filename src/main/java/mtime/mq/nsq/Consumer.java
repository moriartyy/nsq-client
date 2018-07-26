package mtime.mq.nsq;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.lookup.Lookup;
import mtime.mq.nsq.netty.NettyChannel;
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

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.create("nsqConsumerScheduler"));
    private final Lookup lookup;

    public Consumer(ConsumerConfig config) {
        validateConfig(config);
        this.config = config;
        this.lookup = config.getLookup();
        if (this.config.getLookupPeriodMillis() > 0) {
            this.scheduler.scheduleAtFixedRate(this::maintenanceSubscriptions,
                    this.config.getLookupPeriodMillis(), this.config.getLookupPeriodMillis(), TimeUnit.MILLISECONDS);
        }
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
            throw new NSQException("Subscription(topic=" + topic + ", channel=" + channel + ") already exist");
        }

        this.initSubscription(subscription);
    }

    private void initSubscription(Subscription subscription) {
        updateSubscription(
                subscription, subscriptions.computeIfAbsent(subscription, s -> new CopyOnWriteArrayList<>()), true);
    }

    private void validateConfig(ConsumerConfig config) {
        Objects.requireNonNull(config.getLookup(), "lookup");
    }

    private void maintenanceSubscriptions() {
        subscriptions.forEach((subscription, channels) -> {
            try {
                updateSubscription(subscription, channels, false);
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

    private void updateSubscription(Subscription subscription, List<Channel> channels, boolean isNewSubscription) {

        Set<ServerAddress> found = lookup(subscription.getTopic());

        if (found.isEmpty()) {
            log.error("No servers found for topic '{}'", subscription.getTopic());
        } else {
            disconnectAndRemoveObsoletedServers(found, channels);
            connectToAbsentServers(subscription, found, channels);
        }

        if (!isNewSubscription) {
            reconnectToDisconnectedServers(subscription, channels);
        }

        updateReadyCountForChannels(subscription.getMaxInFlight(), channels);
    }

    private void reconnectToDisconnectedServers(Subscription subscription, List<Channel> channels) {
        List<ServerAddress> disconnectedServers = removeDisconnectedServers(channels);
        disconnectedServers.forEach(s -> tryCreateChannel(subscription, s, channels));
    }

    private List<ServerAddress> removeDisconnectedServers(List<Channel> channels) {
        List<Channel> disconnectedChannels = channels.stream()
                .filter(c -> !c.isConnected()).collect(Collectors.toList());

        channels.removeAll(disconnectedChannels);

        return disconnectedChannels.stream().map(Channel::getRemoteAddress).collect(Collectors.toList());
    }

    private void connectToAbsentServers(Subscription subscription, Set<ServerAddress> servers, List<Channel> channels) {
        Set<ServerAddress> absentServers = new HashSet<>(servers);
        channels.forEach(c -> absentServers.remove(c.getRemoteAddress()));

        absentServers.forEach(s -> tryCreateChannel(subscription, s, channels));
    }

    private void tryCreateChannel(Subscription subscription, ServerAddress s, List<Channel> channels) {
        try {
            channels.add(createChannel(subscription, s));
        } catch (Exception e) {
            log.error("Failed to open channel from address {}", s, e);
        }
    }

    private void disconnectAndRemoveObsoletedServers(Set<ServerAddress> servers, List<Channel> channels) {
        List<Channel> obsoletedChannels = channels.stream()
                .filter(c -> !servers.contains(c.getRemoteAddress())).collect(Collectors.toList());

        obsoletedChannels.forEach(CloseableUtils::closeQuietly);

        channels.removeAll(obsoletedChannels);
    }

    private Channel createChannel(Subscription subscription, final ServerAddress address) {
        Channel channel = NettyChannel.open(address, config);
        channel.setMessageHandler(new ConsumerMessageHandler(subscription));
        Response response = channel.sendSubscribe(subscription.getTopic(), subscription.getChannel());
        if (response.getStatus() == Response.Status.ERROR) {
            throw new NSQException("Subscribe failed reason: " + response.getMessage());
        }
        return channel;
    }

    private void closeSubscriptions() {
        this.subscriptions.forEach((subscription, channels) -> {

            // send "CLS" to nsq, so nsq will stop pushing messages
            channels.forEach(channel -> {
                Response response = channel.sendClose();
                if (response.getStatus() == Response.Status.ERROR) {
                    log.error("Clean close failed, reason: {}", response.getMessage());
                }
            });

            // waiting for received messages to be processed
            subscription.getExecutor().shutdown();

            // close channels
            channels.forEach(CloseableUtils::closeQuietly);
        });
    }

    private Set<ServerAddress> lookup(String topic) {
        try {
            return this.lookup.lookup(topic);
        } catch (Exception e) {
            log.error("Look up servers for topic '{}' failed", e);
            return Collections.emptySet();
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
                    log.error("Process message failed, id={}, topic={}, channel={}",
                            new String(message.getId()), this.topic, this.channel, e);
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
