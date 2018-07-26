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
    private int connectionsPerServer = 2;

    @Override
    public String getClientId() {
        String clientId = super.getClientId();
        if (clientId.startsWith("producer")) {
            return clientId;
        }
        return "producer/" + clientId;
    }
}
