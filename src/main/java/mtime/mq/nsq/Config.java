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
    public static final int DEFAULT_SOCKET_THREADS = Runtime.getRuntime().availableProcessors();

    public static final int DISABLE_SAMPLE_RATE = 0;
    public static final long LOOKUP_PERIOD_NEVER = 0L;
    public static final long DEFAULT_LOOKUP_PERIOD_IN_MILLIS = 60 * 1000L;
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_IM_MILLIS = 60 * 1000L;

    public enum Compression {NO_COMPRESSION, DEFLATE, SNAPPY}

    private String clientId = DEFAULT_CLIENT_ID;
    private String hostname = DEFAULT_HOST_NAME;
    private boolean featureNegotiation = true;
    private Integer heartbeatIntervalInMillis = null;
    private long heartbeatTimeoutInMillis = DEFAULT_HEARTBEAT_TIMEOUT_IM_MILLIS;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeoutInMillis = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = DISABLE_SAMPLE_RATE;
    private String userAgent = "JavaClient/0.1";
    private Integer msgTimeoutInMillis = null;
    private SslContext sslContext = null;
    private Lookup lookup;
    private long lookupPeriodMills = DEFAULT_LOOKUP_PERIOD_IN_MILLIS;
    private int socketThreads = DEFAULT_SOCKET_THREADS;

}
