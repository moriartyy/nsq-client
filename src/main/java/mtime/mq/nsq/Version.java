package mtime.mq.nsq;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class Version {

    private static final String DEFAULT_VERSION = "1.0";

    private static String version;

    static {
        Properties properties = new Properties();
        try {
            properties.load(Version.class.getClassLoader().getResourceAsStream("version"));
        } catch (IOException e) {
            log.error("Failed to load version from disk");
        }
        version = properties.getProperty("version", DEFAULT_VERSION);
    }

    public static String get() {
        return version;
    }
}
