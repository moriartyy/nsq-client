package mtime.mq.nsq;

import io.netty.handler.ssl.SslContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.lookup.Lookup;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@Getter
@Setter
public class Config {

    static {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            DEFAULT_CLIENT_ID = localhost.getHostName();
            DEFAULT_HOST_NAME = localhost.getCanonicalHostName();
        } catch (UnknownHostException e) {
            log.warn("Local host name could not resolved", e);
            DEFAULT_CLIENT_ID = "unknown";
            DEFAULT_HOST_NAME = "unknown";
        }
    }

    private static String DEFAULT_CLIENT_ID;
    private static String DEFAULT_HOST_NAME;

    public enum Compression {NO_COMPRESSION, DEFLATE, SNAPPY}

    private String clientId = DEFAULT_CLIENT_ID;
    private String hostname = DEFAULT_HOST_NAME;
    private boolean featureNegotiation = true;
    private Integer heartbeatIntervalInMillis = null;
    private long heartbeatTimeoutInMillis = 60 * 1000L;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeoutMillis = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = 0;
    private String userAgent = "JavaClient/0.1";
    private Integer msgTimeoutMillis = null;
    private SslContext sslContext = null;
    private Lookup lookup;
    private long lookupPeriodMillis = 60 * 1000L;
    private int socketThreads = 2;
    private long responseTimeoutMillis = 5000L;
    private long sendTimeoutMillis = 1000L;
    private int responseQueueSize = 100;

}
