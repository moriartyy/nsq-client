package com.github.brainlag.nsq.channel;

import com.github.brainlag.nsq.*;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.Frame;
import com.github.brainlag.nsq.frames.MessageFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.rmi.runtime.Log;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
public abstract class AbstractChannel implements Channel {
    private static final Logger LOGGER = LogManager.getLogger(AbstractChannel.class);
    private BlockingDeque<ResponseHandler> responseHandlers = new LinkedBlockingDeque<>(10);
    private final ServerAddress serverAddress;
    private final Config config;
    private AtomicInteger leftMessages = new AtomicInteger(0);
    private MessageHandler messageHandler;

    public AbstractChannel(ServerAddress serverAddress, Config config) {
        this.serverAddress = serverAddress;
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public int getLeftMessages() {
        return this.leftMessages.get();
    }

    @Override
    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public ServerAddress getRemoteServerAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        try {
            Response response = sendAndWait(Command.startClose());
            if (response.getStatus() != Response.Status.OK) {
                LOGGER.warn("Received error response: {}", response.getMessage());
            }
        } catch (Exception e) {
            LOGGER.warn("Caught exception when close client", e);
        } finally {
            this.doClose();
        }
    }

    protected abstract void doClose();

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void send(Command command) {
        LOGGER.info("Sending command {}", command.getLine().substring(0, command.getLine().length() - 1));
        doSend(command);
    }

    protected abstract void doSend(Command command);

    @Override
    public synchronized Response sendAndWait(Command command) throws TimeoutException, InterruptedException {
        if (command == Command.NOP) {
            send(command);
        }
        ResponseHandler responseHandler = new ResponseHandler();
        queueResponseHandler(responseHandler);

        try {
            send(command);
        } catch (Exception e) {
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
    public void ready(int size) {
        this.leftMessages.set(size);
        send(Command.ready(size));
    }

    public void receive(Frame frame) {
        if (frame instanceof ResponseFrame) {
            ResponseFrame response = (ResponseFrame) frame;
            if (response.isHeartbeat()) {
                send(Command.NOP);
            } else {
                handleResponse(Response.ok(response.getMessage()));
            }
        } else if (frame instanceof ErrorFrame) {
            handleResponse(Response.error(((ErrorFrame) frame).getErrorMessage()));
        } else if (frame instanceof MessageFrame) {
            handleMessage(toMessage((MessageFrame) frame));
        }
    }

    private void handleMessage(Message message) {
        if (messageHandler != null) {
            leftMessages.getAndIncrement();
            try {
                this.messageHandler.process(message);
            } catch (Exception e) {
                LOGGER.error("Process message failed", e);
            }
        }
    }

    private void handleResponse(Response response) {
        LOGGER.debug("Received response: {}", response.getMessage());
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
        }

        Response getResponse() throws TimeoutException, InterruptedException {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new TimeoutException("Future did not complete in time");
            }
            return this.response;
        }
    }
}
