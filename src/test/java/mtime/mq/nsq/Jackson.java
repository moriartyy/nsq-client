package mtime.mq.nsq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author hongmiao.yu
 */
public class Jackson {
    private static ObjectMapper mapper = new ObjectMapper();

    public static String prettyPrint(Object o) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
