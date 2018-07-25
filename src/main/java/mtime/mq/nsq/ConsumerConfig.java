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

    @Override
    public String getClientId() {
        String clientId = super.getClientId();
        if (clientId.startsWith("producer")) {
            return clientId;
        }
        return "producer/" + clientId;
    }

}