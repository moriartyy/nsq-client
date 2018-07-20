package com.github.brainlag.nsq.netty;

import com.github.brainlag.nsq.Config;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.channel.Channel;
import com.github.brainlag.nsq.channel.ChannelPool;
import com.github.brainlag.nsq.exceptions.NSQException;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.concurrent.Future;

/**
 * @author walter
 */
public class NettyChannelPool implements ChannelPool {
    private final io.netty.channel.pool.ChannelPool channelPool;
    private final ServerAddress serverAddress;
    private final Config config;

    public NettyChannelPool(ServerAddress serverAddress, Config config, long acquireTimeoutMs, int maxConnections) {
        this.serverAddress = serverAddress;
        this.config = config;
        this.channelPool = createPool(acquireTimeoutMs, maxConnections);
    }

    private io.netty.channel.pool.ChannelPool createPool(long acquireTimeoutMs, int maxConnections) {
        return new FixedChannelPool(
                NettyHelper.createBootstrap(serverAddress),
                new NettyChannelPoolHandler(),
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL, acquireTimeoutMs,
                maxConnections, 100);
    }

    @Override
    public Channel acquire() {
        Future<io.netty.channel.Channel> future = channelPool.acquire().awaitUninterruptibly();
        if (!future.isSuccess()) {
            throw new NSQException("Failed to acquire client", future.cause());
        }
        return getOrCreateClient(future.getNow());
    }

    private Channel getOrCreateClient(io.netty.channel.Channel channel) {
        if (channel.hasAttr(NettyChannel.CHANNEL_KEY)) {
            return channel.attr(NettyChannel.CHANNEL_KEY).get();
        }

        return createChannel(channel);
    }

    private NettyChannel createChannel(io.netty.channel.Channel channel) {
        return NettyChannel.instance(channel, this.serverAddress, this.config);
    }

    @Override
    public void release(Channel channel) {
        this.channelPool.release(((NettyChannel) channel).getChannel());
    }

    @Override
    public void close() {
        this.channelPool.close();
    }

    /**
     * @author hongmiao.yu
     */
    public class NettyChannelPoolHandler extends AbstractChannelPoolHandler implements ChannelPoolHandler {

        @Override
        public void channelCreated(io.netty.channel.Channel channel) throws Exception {
            NettyHelper.initChannel(channel);
        }
    }
}
