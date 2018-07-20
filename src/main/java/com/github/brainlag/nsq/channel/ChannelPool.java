package com.github.brainlag.nsq.channel;

/**
 * @author walter
 */
public interface ChannelPool {

    Channel acquire();


    void release(Channel channel);


    void close();


    class PooledChannel extends DelegateChannel {

        private final ChannelPool pool;

        public PooledChannel(Channel delegate, ChannelPool pool) {
            super(delegate);
            this.pool = pool;
        }

        @Override
        public void close() {
            this.pool.release(this);
        }
    }
}
