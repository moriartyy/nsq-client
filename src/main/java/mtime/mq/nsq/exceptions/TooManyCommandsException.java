package mtime.mq.nsq.exceptions;

/**
 * @author hongmiao.yu
 */
public class TooManyCommandsException extends NSQException {

    public TooManyCommandsException(String message) {
        super(message);
    }

    public TooManyCommandsException(Throwable cause) {
        super(cause);
    }

    public TooManyCommandsException(String message, Throwable cause) {
        super(message, cause);
    }
}
