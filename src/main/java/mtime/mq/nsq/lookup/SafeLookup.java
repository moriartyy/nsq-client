package mtime.mq.nsq.lookup;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.ServerAddress;

import java.util.Collections;
import java.util.Set;

/**
 * @author walter
 */
public interface SafeLookup extends Lookup {

    static Lookup from(Lookup lookup) {
        if (lookup instanceof SafeLookup) {
            return lookup;
        }
        return new Wrapper(lookup);
    }

    @Slf4j
    class Wrapper implements SafeLookup {

        private Lookup lookup;

        Wrapper(Lookup lookup) {
            this.lookup = lookup;
        }

        @Override
        public Set<ServerAddress> lookup(String topic) {
            Set<ServerAddress> servers = Collections.emptySet();
            try {
                servers = lookup.lookup(topic);
            } catch (Exception e) {
                log.error("Lookup for topic '{}' failed", topic, e);
            }
            log.debug("Lookup for topic '{}' found: {}", topic, servers);
            return servers;
        }
    }
}
