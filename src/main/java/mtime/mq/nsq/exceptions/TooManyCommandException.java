package mtime.mq.nsq.exceptions;

/**
 * @author hongmiao.yu
 */
public class TooManyCommandException extends NSQException {

    public TooManyCommandException(String message) {
        super(message);
    }

    public TooManyCommandException(Throwable cause) {
        super(cause);
    }

    public TooManyCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
