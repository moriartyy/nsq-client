package mtime.mq.nsq.netty;

import io.netty.channel.EventLoop;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.concurrent.Future;
import mtime.mq.nsq.Config;
import mtime.mq.nsq.ServerAddress;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.channel.ChannelPool;
import mtime.mq.nsq.exceptions.NSQException;

/**
 * @author walter
 */
public class NettyChannelPool implements ChannelPool {
    private final io.netty.channel.pool.ChannelPool channelPool;
    private final ServerAddress serverAddress;
    private final Config config;

    public NettyChannelPool(ServerAddress serverAddress, Config config,
                            long connectionTimeoutMillis, int connectionsPerServer) {
        this.serverAddress = serverAddress;
        this.config = config;
        this.channelPool = new FixedChannelPool(
                NettyHelper.createBootstrap(serverAddress, config),
                new NettyChannelPoolHandler(config),
                new NettyChannelHealthChecker(),
                FixedChannelPool.AcquireTimeoutAction.FAIL, connectionTimeoutMillis,
                connectionsPerServer, 100);
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
        Channel c = extract(channel);
        return c == null ? createChannel(channel) : c;
    }

    private NettyChannel extract(io.netty.channel.Channel channel) {
        if (channel.hasAttr(NettyChannel.CHANNEL_KEY)) {
            return channel.attr(NettyChannel.CHANNEL_KEY).get();
        }
        return null;
    }

    private NettyChannel createChannel(io.netty.channel.Channel channel) {
        return NettyChannel.wrap(channel, this.serverAddress, this.config);
    }

    @Override
    public void release(Channel channel) {
        this.channelPool.release(((NettyChannel) channel).getChannel()).awaitUninterruptibly();
    }

    @Override
    public void close() {
        this.channelPool.close();
    }

    /**
     * @author hongmiao.yu
     */
    class NettyChannelPoolHandler extends AbstractChannelPoolHandler implements ChannelPoolHandler {

        private final Config config;

        public NettyChannelPoolHandler(Config config) {
            this.config = config;
        }

        @Override
        public void channelCreated(io.netty.channel.Channel channel) throws Exception {
            NettyHelper.initChannel(channel, config);
        }
    }

    class NettyChannelHealthChecker implements ChannelHealthChecker {

        @Override
        public Future<Boolean> isHealthy(io.netty.channel.Channel channel) {
            Channel c = extract(channel);
            EventLoop loop = channel.eventLoop();
            return (c == null ? channel.isActive() : c.isConnected()) ?
                    loop.newSucceededFuture(Boolean.TRUE) : loop.newSucceededFuture(Boolean.FALSE);
        }
    }
}
