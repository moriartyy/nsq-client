package mtime.mq.nsq.channel;

import java.io.Closeable;

/**
 * @author walter
 */
public interface ChannelPool extends Closeable {

    Channel acquire();


    void release(Channel channel);


    void close();

}
