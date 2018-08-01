package mtime.mq.nsq.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import mtime.mq.nsq.Config;
import mtime.mq.nsq.ServerAddress;
import mtime.mq.nsq.exceptions.NoConnectionsException;

/**
 * @author hongmiao.yu
 */
public class NettyHelper {

    public static Bootstrap createBootstrap(ServerAddress serverAddress, Config config) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup(config.getSocketThreads()));
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NettyChannelInitializer(config));
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) config.getConnectionTimeoutMillis());
        bootstrap.option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay());
        bootstrap.remoteAddress(serverAddress.getHost(), serverAddress.getPort());
        return bootstrap;
    }

    public static void initChannel(Channel channel, Config config) {
        ChannelPipeline pipeline = channel.pipeline();

        LengthFieldBasedFrameDecoder dec = new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES);
        dec.setSingleDecode(true);

        pipeline.addLast("LengthFieldBasedFrameDecoder", dec);
        pipeline.addLast("NSQDecoder", new NettyDecoder()); // in
        pipeline.addLast("NSQEncoder", new NettyEncoder()); // out
        pipeline.addLast("FeatureDetectionHandler", new NettyFeatureDetectionHandler(config));
        pipeline.addLast("NSQHandler", new NettyHandler()); // in
    }

    public static Channel openChannel(ServerAddress serverAddress, Config config) {
        Bootstrap bootstrap = createBootstrap(serverAddress, config);
        ChannelFuture future = bootstrap.connect();
        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        return future.channel();
    }
}
