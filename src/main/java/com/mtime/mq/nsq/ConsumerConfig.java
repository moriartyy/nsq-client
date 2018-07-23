package com.mtime.mq.nsq;

import lombok.Getter;
import lombok.Setter;

/**
 * @author hongmiao.yu
 */
@Getter
@Setter
public class ConsumerConfig extends Config {
    public static final int MAX_IN_FLIGHT_ADAPTIVE = 0;
    public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private String topic;
    private String channel;
    private int threads = DEFAULT_THREADS;
    private Integer maxInFlight = MAX_IN_FLIGHT_ADAPTIVE;
    private boolean fastFinish = false;

    public ConsumerConfig() {
        setLookupPeriodMills(60 * 1000L);
    }

}
