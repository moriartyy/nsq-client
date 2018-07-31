package mtime.mq.nsq.channel;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.*;
import mtime.mq.nsq.exceptions.NSQExceptions;
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
    private final ServerAddress remoteAddress;
    private final long responseTimeoutMillis;
    private final long sendTimeoutMillis;
    private final int responseQueueSize;
    private MessageHandler messageHandler;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile int readyCount = 0;
    private volatile long lastHeartbeatTimeMillis;

    public AbstractChannel(ServerAddress remoteAddress, Config config) {
        this.remoteAddress = remoteAddress;
        this.responseTimeoutMillis = config.getResponseTimeoutMillis();
        this.heartbeatTimeoutMillis = config.getHeartbeatTimeoutInMillis();
        this.responseQueueSize = config.getResponseQueueSize();
        this.responseHandlers = new LinkedBlockingDeque<>(responseQueueSize);
        this.sendTimeoutMillis = config.getSendTimeoutMillis();
    }

    protected void identity(Config config) {
        try {
            Response response = this.sendAndWait(Command.identify(config));
            if (!response.isOk()) {
                throw NSQExceptions.identifyFailed("Identify failed, reason: " + response.getMessage());
            }
        } catch (Exception e) {
            throw NSQExceptions.identifyFailed("Identify failed with  " + this.getRemoteAddress(), e);
        }
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
    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public ServerAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public boolean isConnected() {
        return !this.isHeartbeatTimeout();
    }

    @Override
    public void send(Command command) {
        log.debug("Send command to {}: '{}'", this.remoteAddress, command.getLine());
        doSend(command, this.sendTimeoutMillis);
    }

    protected abstract void doSend(Command command, long sendTimeoutMillis);

    @Override
    public synchronized Response sendAndWait(Command command) {
        ResponseHandler responseHandler = new ResponseHandler(responseTimeoutMillis);
        queueResponseHandler(responseHandler);
        send(command);
        return responseHandler.getResponse();
    }

    private boolean isHeartbeatTimeout() {
        if (this.lastHeartbeatTimeMillis == 0L) {
            this.lastHeartbeatTimeMillis = System.currentTimeMillis();
            return false;
        }
        return System.currentTimeMillis() - this.lastHeartbeatTimeMillis > this.heartbeatTimeoutMillis;
    }

    private void queueResponseHandler(ResponseHandler responseHandler) {
        if (!responseHandlers.offer(responseHandler)) {
            throw NSQExceptions.tooManyCommands("Too many commands to " + remoteAddress + "(" + responseQueueSize + ")");
        }
    }

    @Override
    public void sendReady(int count) {
        this.readyCount = count;
        Channel.super.sendReady(count);
    }

    @Override
    public void sendRequeue(byte[] messageId) {
        Channel.super.sendRequeue(messageId);
    }

    @Override
    public void sendRequeue(byte[] messageId, long timeoutMS) {
        Channel.super.sendRequeue(messageId, timeoutMS);
        this.inFlight.getAndDecrement();
    }

    @Override
    public void sendFinish(byte[] messageId) {
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
        log.debug("Received response from {}: {}", this.remoteAddress, response.getMessage());
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
            log.debug("Received message from {}: {}", this.remoteAddress, new String(message.getMessageId()));
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
        private final long timeoutMillis;
        private CountDownLatch latch = new CountDownLatch(1);
        private Response response;

        ResponseHandler(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
            this.deadline = System.currentTimeMillis() + timeoutMillis;
        }

        void onResponse(Response response) {
            this.response = response;
            this.latch.countDown();
        }

        Response getResponse() {
            long waitTime = deadline - System.currentTimeMillis();

            if (waitTime <= 0L) {
                throw NSQExceptions.timeout("No response returned in " + timeoutMillis + "ms", remoteAddress);
            }

            try {
                if (!latch.await(waitTime, TimeUnit.MILLISECONDS)) {
                    throw NSQExceptions.timeout("No response returned in " + timeoutMillis + "ms", remoteAddress);
                }
            } catch (InterruptedException e) {
                throw NSQExceptions.interrupted(e);
            }

            return this.response;
        }
    }
}
