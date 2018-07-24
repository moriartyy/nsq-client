package com.mtime.mq.nsq.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;

/**
 * @author hongmiao.yu
 */
public class CloseableUtils {

    protected static final Logger LOGGER = LogManager.getLogger(CloseableUtils.class);

    public static void closeQuietly(Closeable c) {
        try {
            c.close();
        } catch (Exception e) {
            LOGGER.error("Exception caught", e);
        }
    }
}
