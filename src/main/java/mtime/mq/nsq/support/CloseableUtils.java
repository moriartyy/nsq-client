package mtime.mq.nsq.support;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class CloseableUtils {

    public static void closeQuietly(Closeable c) {
        try {
            c.close();
        } catch (Exception e) {
            log.error("Exception caught", e);
        }
    }
}
