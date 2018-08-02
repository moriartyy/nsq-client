package mtime.mq.nsq;

import mtime.mq.nsq.exceptions.NSQExceptions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author hongmiao.yu
 */
public class ResponseFuture implements Future<Response> {

    private final ServerAddress server;
    private volatile Response response;
    private CountDownLatch latch = new CountDownLatch(1);

    public ResponseFuture(ServerAddress server) {
        this.server = server;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return response != null;
    }

    @Override
    public Response get() {
        if (!isDone()) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw NSQExceptions.interrupted(e);
            }
        }
        return response;
    }

    public Response get(long timeoutMillis) {
        return get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public Response get(long timeout, TimeUnit unit) {
        if (!isDone()) {
            try {
                if (!latch.await(timeout, unit)) {
                    throw NSQExceptions.timeout("No response returned after " + unit.toMillis(timeout) + " ms from server" + server);
                }
            } catch (InterruptedException e) {
                throw NSQExceptions.interrupted(e);
            }
        }
        return response;
    }

    public void set(Response response) {
        this.response = response;
        this.latch.countDown();
    }
}
