package mtime.mq.nsq;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
@Getter
@Setter
public class ConsumerConfig extends Config {

    private long reconnectIntervalMillis = TimeUnit.MINUTES.toMillis(1);

    @Override
    public String getClientId() {
        String clientId = super.getClientId();
        if (clientId.startsWith("consumer")) {
            return clientId;
        }
        return "consumer/" + clientId;
    }

}
