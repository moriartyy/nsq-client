package mtime.mq.nsq;

import lombok.Getter;
import lombok.Setter;

/**
 * @author hongmiao.yu
 */
@Getter
@Setter
public class ProducerConfig extends Config {
    private long connectionTimeoutMillis = 3000L;
    private int maxConnectionsPerServer = 3;
}
