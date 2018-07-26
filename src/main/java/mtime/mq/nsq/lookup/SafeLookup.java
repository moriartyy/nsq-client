package mtime.mq.nsq.lookup;

import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * @author walter
 */
public interface SafeLookup extends Lookup {


    static Lookup from(Lookup lookup) {
        if (lookup instanceof SafeLookup) {
            return lookup;
        }
        return (SafeLookup) topic -> {
            try {
                return lookup.lookup(topic);
            } catch (Exception e) {
                LoggerFactory.getLogger(SafeLookup.class).error("Lookup for topic '{}' failed", topic, e);
            }
            return Collections.emptySet();
        };
    }
}
