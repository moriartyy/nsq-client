package com.mtime.mq.nsq;

import lombok.Getter;
import lombok.Setter;

/**
 * @author hongmiao.yu
 */
@Getter
@Setter
public class ConsumerConfig extends Config {

    public ConsumerConfig() {
        setLookupPeriodMills(60 * 1000L);
    }

}
