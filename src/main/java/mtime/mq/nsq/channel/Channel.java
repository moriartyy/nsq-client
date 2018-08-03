package mtime.mq.nsq.channel;

import mtime.mq.nsq.*;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.exceptions.NSQExceptions;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author hongmiao.yu
 */
public interface Channel extends Closeable {

    int getUnconfirmedCommands();

    int getReadyCount();

    int getInFlight();

    void setMessageHandler(MessageHandler messageHandler);

    ServerAddress getRemoteAddress();

    CompletableFuture<Response> send(Command command);

    default Response send(Command command, long timeoutMillis) {
        CompletableFuture<Response> r = send(command);
        try {
            return r.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw NSQExceptions.interrupted(e);
        } catch (ExecutionException e) {
            throw NSQException.propagate(e);
        } catch (TimeoutException e) {
            throw NSQExceptions.timeout("No response returned after " + timeoutMillis + "ms");
        }
    }

    void close();

    boolean isConnected();

    default CompletableFuture<Response> sendReady(int count) {
        return send(Commands.ready(count));
    }

    default CompletableFuture<Response> sendRequeue(byte[] messageId) {
        return sendRequeue(messageId, 0L);
    }

    default CompletableFuture<Response> sendRequeue(byte[] messageId, long timeoutMS) {
        return send(Commands.requeue(messageId, timeoutMS));
    }

    default CompletableFuture<Response> sendFinish(byte[] messageId) {
        return send(Commands.finish(messageId));
    }

    default CompletableFuture<Response> sendTouch(byte[] messageId) {
        return send(Commands.touch(messageId));
    }

    default CompletableFuture<Response> sendSubscribe(String topic, String channel) {
        return send(Commands.subscribe(topic, channel));
    }

    default CompletableFuture<Response> sendClose() {
        return send(Commands.startClose());
    }
}
