package mtime.mq.nsq.exceptions;

import mtime.mq.nsq.ServerAddress;

/**
 * @author hongmiao.yu
 */
public class NSQExceptions {

    public static InterruptedException interrupted(Throwable cause) {
        return new InterruptedException(cause);
    }

    public static TimeoutException timeout(String message, ServerAddress remoteAddress) {
        return new TimeoutException(message(message, remoteAddress));
    }

    public static TimeoutException timeout(String message) {
        return new TimeoutException(message);
    }

    private static String message(String message, ServerAddress remoteAddress) {
        return message + ", remoteAddress=" + remoteAddress;
    }

    public static TooManyCommandsException tooManyCommands(String message) {
        return new TooManyCommandsException(message);
    }

    public static IdentifyFailedException identifyFailed(String message, Throwable cause) {
        return new IdentifyFailedException(message, cause);
    }
}
