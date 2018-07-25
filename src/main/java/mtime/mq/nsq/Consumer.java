package mtime.mq.nsq;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.netty.NettyChannel;
import mtime.mq.nsq.support.CloseableUtils;
import mtime.mq.nsq.support.DaemonThreadFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class Consumer implements Closeable {

    private final ConsumerConfig config;
    private final Map<Subscription, List<Channel>> subscriptions = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory.create("nsqConsumerScheduler"));
    private Executor defaultExecutor;

    public Consumer(ConsumerConfig config) {
        validateConfig(config);
        this.config = config;
        if (this.config.getLookupPeriodMills() != Config.LOOKUP_PERIOD_NEVER) {
            this.scheduler.scheduleAtFixedRate(this::maintenanceSubscriptions,
                    this.config.getLookupPeriodMills(), this.config.getLookupPeriodMills(), TimeUnit.MILLISECONDS);
        }
    }

    private synchronized Executor getDefaultExecutor() {
        if (this.defaultExecutor == null) {
            this.defaultExecutor = newExecutor(0);
        }
        return this.defaultExecutor;
    }

    private Executor newExecutor(int threads) {
        return new Executor.DefaultImpl(threads);
    }

    public void subscribe(String topic, String channel, MessageHandler messageHandler) {
        subscribe(topic, channel, messageHandler, null, 0);
    }

    public void subscribe(String topic, String channel, MessageHandler messageHandler, int numberOfThreads) {
        subscribe(topic, channel, messageHandler, newExecutor(numberOfThreads), 0);
    }

    public void subscribe(String topic, String channel, MessageHandler messageHandler, Executor executor, int maxInFlight) {
        Subscription subscription = Subscription.builder()
                .topic(topic)
                .channel(channel)
                .handler(messageHandler)
                .executor(executor == null ? getDefaultExecutor() : executor)
                .maxInFlight(maxInFlight)
                .build();

        if (subscriptions.containsKey(subscription)) {
            throw new NSQException("Subscription(topic=" + topic + ", channel=" + channel + ") already exist");
        }

        this.initSubscription(subscription);
    }

    private void initSubscription(Subscription subscription) {
        updateSubscription(subscription, subscriptions.computeIfAbsent(subscription, s -> new CopyOnWriteArrayList<>()));
    }

    private void validateConfig(ConsumerConfig config) {
        Objects.requireNonNull(config.getLookup(), "lookup");
    }

    private void maintenanceSubscriptions() {
        subscriptions.forEach((subscription, channels) -> {
            try {
                removeDisconnectedChannels(channels);
                updateSubscription(subscription, channels);
            } catch (Exception e) {
                log.error("Exception caught while maintenance subscription(topic={}, channel={})",
                        subscription.getTopic(), subscription.getChannel(), e);
            }
        });
    }

    private void updateReadyCountForChannels(int maxInFlight, List<Channel> channels) {
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

    private void removeDisconnectedChannels(List<Channel> channels) {
        channels.removeIf(c -> !c.isConnected());
    }

    private void updateSubscription(Subscription subscription, List<Channel> channels) {
        Set<ServerAddress> found = lookup(subscription.getTopic());
        if (found.isEmpty()) {
            return;
        }

        removeObsoletedChannels(found, channels);
        openChannelToAbsentServers(subscription, found, channels);
        updateReadyCountForChannels(subscription.getMaxInFlight(), channels);
    }

    private void openChannelToAbsentServers(Subscription subscription, Set<ServerAddress> addresses, List<Channel> channels) {
        Set<ServerAddress> absentAddresses = new HashSet<>(addresses);
        channels.forEach(c -> absentAddresses.remove(c.getRemoteAddress()));
        absentAddresses.forEach(s -> {
            try {
                channels.add(createChannel(subscription, s));
            } catch (Exception e) {
                log.error("Failed to open channel from address {}", s, e);
            }
        });
    }

    private void removeObsoletedChannels(Set<ServerAddress> addresses, List<Channel> channels) {
        Iterator<Channel> iterator = channels.iterator();
        while (iterator.hasNext()) {
            Channel c = iterator.next();
            if (!addresses.contains(c.getRemoteAddress())) {
                CloseableUtils.closeQuietly(c);
                iterator.remove();
            }
        }
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
        return this.config.getLookup().lookup(topic);
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
