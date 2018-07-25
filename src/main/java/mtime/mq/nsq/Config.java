package mtime.mq.nsq;

import io.netty.handler.ssl.SslContext;
import lombok.Getter;
import lombok.Setter;
import mtime.mq.nsq.lookup.Lookup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Getter
@Setter
public class Config {
    protected static final Logger LOGGER = LogManager.getLogger(Config.class);

    static {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            DEFAULT_CLIENT_ID = localhost.getHostName();
            DEFAULT_HOST_NAME = localhost.getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Local host name could not resolved", e);
            DEFAULT_CLIENT_ID = "unknown";
            DEFAULT_HOST_NAME = "unknown";
        }
    }

    private static String DEFAULT_CLIENT_ID;
    private static String DEFAULT_HOST_NAME;
    public static final int DEFAULT_SOCKET_THREADS = Runtime.getRuntime().availableProcessors();

    public static final int DISABLE_SAMPLE_RATE = 0;
    public static final long LOOKUP_PERIOD_NEVER = 0L;

    public enum Compression {NO_COMPRESSION, DEFLATE, SNAPPY}

    private String clientId = DEFAULT_CLIENT_ID;
    private String hostname = DEFAULT_HOST_NAME;
    private boolean featureNegotiation = true;
    private Integer heartbeatInterval = null;
    private Integer outputBufferSize = null;
    private Integer outputBufferTimeout = null;
    private boolean tlsV1 = false;
    private Compression compression = Compression.NO_COMPRESSION;
    private Integer deflateLevel = null;
    private Integer sampleRate = DISABLE_SAMPLE_RATE;
    private String userAgent = "JavaClient/0.1";
    private Integer msgTimeout = null;
    private SslContext sslContext = null;
    private Lookup lookup;
    private long lookupPeriodMills = LOOKUP_PERIOD_NEVER;
    private int socketThreads = DEFAULT_SOCKET_THREADS;

}
