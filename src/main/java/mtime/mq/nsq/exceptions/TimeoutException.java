package mtime.mq.nsq.exceptions;

import lombok.Getter;

/**
 * @author hongmiao.yu
 */
@Getter
public class TimeoutException extends NSQException {

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(Throwable cause) {
        super(cause);
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

}
