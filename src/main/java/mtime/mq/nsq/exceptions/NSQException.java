package mtime.mq.nsq.exceptions;

public class NSQException extends RuntimeException {

    public NSQException(String message) {
        super(message);
    }

    public NSQException(Throwable cause) {
        super(cause);
    }

    public NSQException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NSQException propagate(Exception e) {
        return (e instanceof NSQException ? (NSQException) e : new NSQException(e));
    }

    public static NSQException instance(String message, Throwable cause) {
        return new NSQException(message, cause);
    }

    public static NSQException instance(String message) {
        return new NSQException(message);
    }
}
