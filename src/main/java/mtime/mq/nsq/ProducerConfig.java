package mtime.mq.nsq;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
@Getter
@Setter
public class ProducerConfig extends Config {

    private long connectionTimeoutMillis = TimeUnit.SECONDS.toMillis(3);
    private int connectionsPerServer = 2;
    private int maxPublishRetries = 1;
    private long haltDurationMillis = TimeUnit.MINUTES.toMillis(1);
    private int maxAcquireConnectionErrorCount = 3;

    @Override
    public String getClientId() {
        String clientId = super.getClientId();
        if (clientId.startsWith("producer")) {
            return clientId;
        }
        return "producer/" + clientId;
    }
}
