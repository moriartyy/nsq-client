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
    private BlockingDeque<ResponseHandler> responseHandlers = new LinkedBlockingDeque<>(10);
    private final ServerAddress serverAddress;
    private final Config config;
    private MessageHandler messageHandler;
    private AtomicInteger inFlight = new AtomicInteger();
    private volatile int ready = 0;

    public AbstractChannel(ServerAddress serverAddress, Config config) {
        this.serverAddress = serverAddress;
        this.config = config;
    }

    @Override
    public int getInFlight() {
        return this.inFlight.get();
    }

    @Override
    public int getReadyCount() {
        return ready;
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
        return false;
    }

    @Override
    public void send(Command command) throws NSQException {
        log.debug("Sending command {}", command.getLine());
        try {
            doSend(command);
        } catch (NSQException e) {
            throw e;
        } catch (Exception e1) {
            throw new NSQException("Send command failed", e1);
        }
    }

    protected abstract void doSend(Command command);

    @Override
    public synchronized Response sendAndWait(Command command) throws NSQException {
        ResponseHandler responseHandler = new ResponseHandler();
        queueResponseHandler(responseHandler);

        try {
            send(command);
        } catch (NSQException e) {
            dequeueResponseHandler(responseHandler);
            throw e;
        }

        return responseHandler.getResponse();
    }

    private void dequeueResponseHandler(ResponseHandler responseHandler) {
        this.responseHandlers.pollLast();
    }

    private void queueResponseHandler(ResponseHandler responseHandler) {
        if (!responseHandlers.offer(responseHandler)) {
            throw new NSQException("Too many commands");
        }
    }

    @Override
    public void sendReady(int count) throws NSQException {
        this.ready = count;
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
            send(Command.NOP);
        } else {
            handleResponse(Response.ok(response.getMessage()));
        }
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
        private CountDownLatch latch = new CountDownLatch(1);
        private Response response;

        ResponseHandler() {
        }

        void onResponse(Response response) {
            this.response = response;
            this.latch.countDown();
        }

        Response getResponse() throws NSQException {
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new NSQException("No response returned before timeout");
                }
            } catch (InterruptedException e) {
                throw new NSQException("Get response is interrupted");
            }
            return this.response;
        }
    }
}
