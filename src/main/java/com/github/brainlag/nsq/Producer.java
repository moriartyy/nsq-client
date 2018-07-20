package com.github.brainlag.nsq;

import com.github.brainlag.nsq.channel.Channel;
import com.github.brainlag.nsq.channel.ChannelPool;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.netty.NettyChannelPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Producer implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(Producer.class);

    private Set<ServerAddress> addresses;
    private int roundRobinCount = 0;
    private Config config;
    private int maxConnectionRetries = 5;
    private final Map<ServerAddress, ChannelPool> clientPools = new ConcurrentHashMap<>();


    public Producer(Set<ServerAddress> addresses) {
        this(addresses, new Config());
    }

    public Producer(Set<ServerAddress> addresses, Config config) {
        this.config = config;
        this.addresses = addresses;
    }

    protected Channel acquireChannel() throws NoConnectionsException {
        int retries = 0;
        while (retries < maxConnectionRetries) {
            ServerAddress serverAddress = nextAddress();
            if (serverAddress == null) {
                throw new IllegalStateException("No server configured for producer");
            }
            try {
                return acquireChannel(serverAddress);
            } catch (Exception ex) {
                if (this.addresses.size() == 1) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ix) {
                        throw new NoConnectionsException("Could not acquire a connection from pool", ix);
                    }
                } else {
                    throw new NoConnectionsException("Could not acquire a connection from pool", ex);
                }
            }
            retries++;
        }
        throw new NoConnectionsException("Could not acquire a connection from pool after " + maxConnectionRetries + " times of retries");
    }

    private Channel acquireChannel(ServerAddress serverAddress) {
        ChannelPool pool = getPool(serverAddress);
        return pool.acquire();
    }

    private ChannelPool getPool(ServerAddress serverAddress) {
        return clientPools.computeIfAbsent(serverAddress,
                s -> new NettyChannelPool(s, this.config, 3000L, 3));
    }

    private ServerAddress nextAddress() {
        if (addresses.isEmpty()) {
            return null;
        }
        ServerAddress[] serverAddresses = addresses.toArray(new ServerAddress[addresses.size()]);
        return serverAddresses[roundRobinCount++ % serverAddresses.length];
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

        sendMessageCommand(Command.multiPublish(topic, messages));
    }

    public void produce(String topic, byte[] message) throws NSQException {
        sendMessageCommand(Command.publish(topic, message));
    }

    public void produceDeferred(String topic, byte[] message, int deferMillis) throws NSQException {
        sendMessageCommand(Command.deferredPublish(topic, message, deferMillis));
    }

    private void sendMessageCommand(Command command) throws NSQException {
        Channel channel = acquireChannel();
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
    }
}
