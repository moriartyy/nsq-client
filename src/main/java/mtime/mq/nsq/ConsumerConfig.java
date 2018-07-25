package mtime.mq.nsq;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author hongmiao.yu
 */
@Builder
@Getter
@Setter
public class ConsumerConfig extends Config {

    public ConsumerConfig() {
        setLookupPeriodMills(60 * 1000L);
    }

}
