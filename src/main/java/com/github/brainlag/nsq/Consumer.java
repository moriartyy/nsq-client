package com.github.brainlag.nsq;

import com.github.brainlag.nsq.channel.Channel;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.lookup.Lookup;
import com.github.brainlag.nsq.netty.NettyChannel;
import com.github.brainlag.nsq.netty.NettyHelper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
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
        this.scheduler.scheduleAtFixedRate(this::connect, 0, lookupPeriod, TimeUnit.MILLISECONDS);
    }

    public int getQueueSize() {
        return this.executor.getQueueSize();
    }

    private Channel createChannel(final ServerAddress serverAddress) {
        try {
            Bootstrap bootstrap = NettyHelper.createBootstrap(serverAddress);
            ChannelFuture future = bootstrap.connect();
            future.awaitUninterruptibly();
            if (!future.isSuccess()) {
                throw new NoConnectionsException("Could not connect to server", future.cause());
            }
            Channel channel = NettyChannel.instance(future.channel(), serverAddress, config);
            channel.setMessageHandler(this::processMessage);
            channel.send(Command.subscribe(this.topic, this.channel));
            channel.send(Command.ready(messagesPerBatch));
            return channel;
        } catch (final NoConnectionsException e) {
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
            message.getChannel().ready(messagesPerBatch);
        }
    }

    private void halt(Channel channel) {
       if(haltedChannels.add(channel))  {
           LOGGER.trace("Backing off, halt connection: " + channel.getRemoteServerAddress());
           channel.ready(0);
       }
    }

    private synchronized void awakenAll() {
        if (haltedChannels.size() > 0) {
            LOGGER.trace("Awaken halt connections");
            try {
                haltedChannels.forEach(channel -> channel.ready(1));
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

    private void connect() {
        for (final Iterator<Map.Entry<ServerAddress, Channel>> it = channels.entrySet().iterator(); it.hasNext(); ) {
            Channel channel = it.next().getValue();
            if (!channel.isConnected()) {
                LOGGER.warn("Remove disconnected connection: " + channel.getRemoteServerAddress());
                channel.close();
                it.remove();
            }
//            if (conn.getHeartbeatTime().plusMinutes(1).isBefore(LocalDateTime.now())) {
//                LOGGER.warn("Remove disconnected connection: " + conn.getServerAddress());
//                conn.close();
//                it.remove();
//            }
        }

        // 防止连接停摆的补漏机制
        if (executor.isIdle()) {
            try {
                int threshold = Math.min(messagesPerBatch, 5);
                channels.forEach((server, channel) -> {
                    if (channel.getLeftMessages() < threshold) {
                        channel.ready(threshold);
                    }
                });
            } catch (Exception e) {
                LOGGER.warn("recover from idle failed", e);
            }
        }

        final Set<ServerAddress> newAddresses = lookupAddresses();
        final Set<ServerAddress> oldAddresses = channels.keySet();

        LOGGER.debug("Addresses NSQ connected to: " + newAddresses);
        if (newAddresses.isEmpty()) {
            // in case the lookup server is not reachable for a short time we don't we dont want to
            // force close connection
            // just log a message and keep moving
            LOGGER.warn("No NSQLookup server connections or topic does not exist.");
        } else {
            for (ServerAddress oldAddress : oldAddresses) {
                if (!newAddresses.contains(oldAddress)) {
                    LOGGER.info("Remove server " + oldAddress.toString());
                    channels.get(oldAddress).close();
                    channels.remove(oldAddress);
                }
            }
            for (ServerAddress newAddress : newAddresses) {
                if (!channels.containsKey(newAddress)) {
                    final Channel channel = createChannel(newAddress);
                    if (channel != null) {
                        channels.put(newAddress, channel);
                    }
                }
            }
        }
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadpool.
     */
//    public NSQConsumer setExecutor(final ExecutorService executor) {
//        if (!started) {
//            this.executor = executor;
//        }
//        return this;
//    }
    private Set<ServerAddress> lookupAddresses() {
        return lookup.lookup(topic);
    }

    /**
     * This method allows for a runnable task to be scheduled using the NSQConsumer's scheduler executor
     * This is intended for calling a periodic method in a NSQMessageCallback for batching messages
     * without needing state in the callback itself
     *
     * @param task   The Runnable task
     * @param delay  Delay in milliseconds
     * @param period Period of time between scheduled runs
     * @param unit   TimeUnit for delay and period times
     * @return ScheduledFuture - useful for cancelling scheduled task
     */
    public ScheduledFuture scheduleRun(Runnable task, int delay, int period, TimeUnit unit) {
        return scheduler.scheduleAtFixedRate(task, delay, period, unit);
    }

    @Override
    public void close() {
        scheduler.shutdown();
        cleanClose();
    }
}
