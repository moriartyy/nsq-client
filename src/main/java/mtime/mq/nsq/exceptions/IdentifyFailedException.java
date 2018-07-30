package mtime.mq.nsq.exceptions;

/**
 * @author hongmiao.yu
 */
public class IdentifyFailedException extends NSQException {

    public IdentifyFailedException(String message) {
        super(message);
    }

    public IdentifyFailedException(Throwable cause) {
        super(cause);
    }

    public IdentifyFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
