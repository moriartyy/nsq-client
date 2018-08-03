package mtime.mq.nsq.exceptions;

/**
 * @author hongmiao.yu
 */
public class IdentifyException extends NSQException {

    public IdentifyException(String message) {
        super(message);
    }

    public IdentifyException(Throwable cause) {
        super(cause);
    }

    public IdentifyException(String message, Throwable cause) {
        super(message, cause);
    }
}
