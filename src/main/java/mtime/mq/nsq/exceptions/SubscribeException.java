package mtime.mq.nsq.exceptions;

/**
 * @author hongmiao.yu
 */
public class SubscribeException extends NSQException {

    public SubscribeException(String message) {
        super(message);
    }

    public SubscribeException(Throwable cause) {
        super(cause);
    }

    public SubscribeException(String message, Throwable cause) {
        super(message, cause);
    }
}
