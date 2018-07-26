package mtime.mq.nsq.channel;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.*;
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.frames.ErrorFrame;
import mtime.mq.nsq.frames.Frame;
import mtime.mq.nsq.frames.MessageFrame;
import mtime.mq.nsq.frames.ResponseFrame;

import java.util.Date;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
@Slf4j
public abstract class AbstractChannel implements Channel {
    private final long heartbeatTimeoutMillis;
    private final BlockingDeque<ResponseHandler> responseHandlers;
    private final ServerAddress serverAddress;
    private final Config config;
    private final long responseTimeoutMillis;
    private final long sendTimeoutMillis;
    private MessageHandler messageHandler;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile int readyCount = 0;
    private volatile long lastHeartbeatTimeMillis;

    public AbstractChannel(ServerAddress serverAddress, Config config) {
        this.serverAddress = serverAddress;
        this.config = config;
        this.responseTimeoutMillis = this.config.getResponseTimeoutMillis();
        this.heartbeatTimeoutMillis = this.config.getHeartbeatTimeoutInMillis();
        this.responseHandlers = new LinkedBlockingDeque<>(this.config.getResponseQueueSize());
        this.sendTimeoutMillis = this.config.getSendTimeoutMillis();
    }

    @Override
    public int getInFlight() {
        return this.inFlight.get();
    }

    @Override
    public int getReadyCount() {
        return readyCount;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public ServerAddress getRemoteAddress() {
        return serverAddress;
    }

    @Override
    public boolean isConnected() {
        return this.isHeartbeatTimeout();
    }

    @Override
    public void send(Command command) throws NSQException {
        log.debug("Sending command {}", command.getLine());
        try {
            doSend(command, this.sendTimeoutMillis);
        } catch (Exception e) {
            throw NSQException.of(e);
        }
    }

    protected abstract void doSend(Command command, long sendTimeoutMillis);

    @Override
    public synchronized Response sendAndWait(Command command) throws NSQException {
        ResponseHandler responseHandler = new ResponseHandler(System.currentTimeMillis() + responseTimeoutMillis);

        queueResponseHandler(responseHandler);

        try {
            send(command);
        } catch (Exception e) {
            this.close();
            throw NSQException.of(e);
        }

        return responseHandler.getResponse();
    }

    private boolean isHeartbeatTimeout() {
        return System.currentTimeMillis() - this.lastHeartbeatTimeMillis < this.heartbeatTimeoutMillis;
    }

    private void queueResponseHandler(ResponseHandler responseHandler) {
        if (!responseHandlers.offer(responseHandler)) {
            throw new NSQException("Too many commands");
        }
    }

    @Override
    public void sendReady(int count) throws NSQException {
        this.readyCount = count;
        Channel.super.sendReady(count);
    }

    @Override
    public void sendRequeue(byte[] messageId) throws NSQException {
        Channel.super.sendRequeue(messageId);
    }

    @Override
    public void sendRequeue(byte[] messageId, long timeoutMS) throws NSQException {
        Channel.super.sendRequeue(messageId, timeoutMS);
        this.inFlight.getAndDecrement();
    }

    @Override
    public void sendFinish(byte[] messageId) throws NSQException {
        Channel.super.sendFinish(messageId);
        this.inFlight.getAndDecrement();
    }

    public void receive(Frame frame) {
        if (frame instanceof ResponseFrame) {
            receiveResponseFrame((ResponseFrame) frame);
        } else if (frame instanceof ErrorFrame) {
            receiveErrorFrame((ErrorFrame) frame);
        } else if (frame instanceof MessageFrame) {
            receiveMessageFrame((MessageFrame) frame);
        }
    }

    private void receiveErrorFrame(ErrorFrame frame) {
        handleResponse(Response.error(frame.getErrorMessage()));
    }

    private void receiveResponseFrame(ResponseFrame response) {
        log.debug("Received response: {}", response.getMessage());
        if (response.isHeartbeat()) {
            handleHeartbeat();
        } else {
            handleResponse(Response.ok(response.getMessage()));
        }
    }

    private void handleHeartbeat() {
        this.lastHeartbeatTimeMillis = System.currentTimeMillis();
        send(Command.NOP);
    }

    private void receiveMessageFrame(MessageFrame message) {
        if (log.isDebugEnabled()) {
            log.debug("Received message: {}", new String(message.getMessageId()));
        }

        if (messageHandler == null) {
            return;
        }

        this.inFlight.getAndIncrement();

        try {
            this.messageHandler.process(toMessage(message));
        } catch (Exception e) {
            log.error("Process message failed, id={}", new String(message.getMessageId()), e);
        }
    }

    private void handleResponse(Response response) {
        ResponseHandler responseHandler = this.responseHandlers.poll();
        if (responseHandler != null) {
            responseHandler.onResponse(response);
        }
    }

    private Message toMessage(MessageFrame msg) {
        final Message message = new Message();
        message.setReceiveTime(new Date());
        message.setAttempts(msg.getAttempts());
        message.setChannel(this);
        message.setId(msg.getMessageId());
        message.setMessage(msg.getMessageBody());
        message.setTimestamp(new Date(TimeUnit.NANOSECONDS.toMillis(msg.getTimestamp())));
        return message;
    }

    class ResponseHandler {
        private final long deadline;
        private CountDownLatch latch = new CountDownLatch(1);
        private Response response;

        ResponseHandler(long deadline) {
            this.deadline = deadline;
        }

        void onResponse(Response response) {
            this.response = response;
            this.latch.countDown();
        }

        Response getResponse() throws NSQException {
            long waitTime = deadline - System.currentTimeMillis();

            if (waitTime <= 0L) {
                throw new NSQException("No response returned before timeout");
            }

            try {
                if (!latch.await(waitTime, TimeUnit.MILLISECONDS)) {
                    throw new NSQException("No response returned before timeout");
                }
            } catch (InterruptedException e) {
                throw new NSQException("Get response is interrupted");
            }

            return this.response;
        }
    }
}
