package com.mtime.mq.nsq;

import java.util.concurrent.*;

/**
 * @author hongmiao.yu
 */
public class ResponseFuture implements Future<Response> {
    private volatile Response response;
    private CountDownLatch latch = new CountDownLatch(1);
    private volatile Throwable cause;

    public void setResponse(Response response) {
        this.response = response;
        this.latch.countDown();
    }

    public void setFailure(Throwable cause) {
        this.cause = cause;
        this.latch.countDown();
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
        return this.response != null || this.cause != null;
    }

    @Override
    public Response get() throws InterruptedException, ExecutionException {
        latch.await();
        return this.response;
    }

    @Override
    public Response get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!latch.await(timeout, unit)) {
            throw new TimeoutException("Future did not complete in time");
        }
        return this.response;
    }

}
