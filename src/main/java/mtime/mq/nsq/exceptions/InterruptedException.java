package mtime.mq.nsq.exceptions;

/**
 * @author hongmiao.yu
 */
public class InterruptedException extends NSQException {

    public InterruptedException(String message) {
        super(message);
    }

    public InterruptedException(Throwable cause) {
        super(cause);
    }

    public InterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
