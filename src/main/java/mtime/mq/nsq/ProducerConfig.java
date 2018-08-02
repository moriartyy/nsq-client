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

    private int connectionsPerServer = 10;
    private int maxPublishRetries = 1;
    private long haltDurationMillis = TimeUnit.MINUTES.toMillis(1);
    private int maxSendErrorCount = 3;

    public ProducerConfig() {
        setSocketThreads(0);
    }

    @Override
    public String getClientId() {
        String clientId = super.getClientId();
        if (clientId.startsWith("producer")) {
            return clientId;
        }
        return "producer/" + clientId;
    }
}
