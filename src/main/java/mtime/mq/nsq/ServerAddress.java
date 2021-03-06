package mtime.mq.nsq;

import java.util.Objects;

public class ServerAddress implements Comparable<ServerAddress> {

    public ServerAddress(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ServerAddress that = (ServerAddress) o;
        return Objects.equals(port, that.port) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    private String host;
    private int port;

    @Override
    public int compareTo(ServerAddress o) {
        int r = this.host.compareTo(o.host);
        if (r == 0) {
            r = Integer.compare(this.port, o.port);
        }
        return r;
    }
}
