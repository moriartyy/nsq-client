package mtime.mq.nsq.channel;

import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.*;
import mtime.mq.nsq.exceptions.NSQExceptions;
import mtime.mq.nsq.frames.ErrorFrame;
import mtime.mq.nsq.frames.Frame;
import mtime.mq.nsq.frames.MessageFrame;
import mtime.mq.nsq.frames.ResponseFrame;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
@Slf4j
public abstract class AbstractChannel implements Channel {
    private static final ConcurrentMap<ServerAddress, AtomicInteger> INSTANCE_COUNTERS = new ConcurrentHashMap<>();
    private final long heartbeatTimeoutMillis;
    private final BlockingDeque<ResponseFuture> responseListeners;
    private final ServerAddress remoteAddress;
    private final int commandQueueSize;
    private MessageHandler messageHandler;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile int readyCount = 0;
    private volatile long lastHeartbeatTimeMillis;

    public AbstractChannel(ServerAddress remoteAddress, Config config) {
        this.remoteAddress = remoteAddress;
        this.heartbeatTimeoutMillis = config.getHeartbeatTimeoutInMillis();
        this.commandQueueSize = config.getCommandQueueSize();
        this.responseListeners = new LinkedBlockingDeque<>(commandQueueSize);
        log.info("Channel created, server: {}, current: {}", remoteAddress, getCounter(remoteAddress).incrementAndGet());
    }

    private AtomicInteger getCounter(ServerAddress serverAddress) {
        return INSTANCE_COUNTERS.computeIfAbsent(serverAddress, s -> new AtomicInteger());
    }

    protected void identity(Config config) {
        try {
            Response response = this.send(Commands.identify(config)).get();
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
    public ResponseFuture send(Command command) {
        ResponseFuture f = new ResponseFuture(remoteAddress);
        send(command, f);
        return f;
    }

    private void send(Command command, ResponseFuture responseFuture) {
        log.debug("Sending command to {}: {}", this.remoteAddress, command.getLine());
        if (command == Commands.NOP) {
            responseFuture.set(Response.VOID);
        } else {
            queue(responseFuture);
        }
        doSend(command, responseFuture);
    }

    protected abstract void doSend(Command command, ResponseFuture responseFuture);

    private boolean isHeartbeatTimeout() {
        if (this.lastHeartbeatTimeMillis == 0L) {
            this.lastHeartbeatTimeMillis = System.currentTimeMillis();
            return false;
        }
        return System.currentTimeMillis() - this.lastHeartbeatTimeMillis > this.heartbeatTimeoutMillis;
    }

    private void queue(ResponseFuture responseListener) {
        if (!responseListeners.offer(responseListener)) {
            throw NSQExceptions.tooManyCommands("Too many commands to " + remoteAddress + "(" + commandQueueSize + ")");
        }
    }

    @Override
    public ResponseFuture sendReady(int count) {
        this.readyCount = count;
        return Channel.super.sendReady(count);
    }

    @Override
    public ResponseFuture sendRequeue(byte[] messageId, long timeoutMS) {
        this.inFlight.getAndDecrement();
        return Channel.super.sendRequeue(messageId, timeoutMS);
    }

    @Override
    public ResponseFuture sendFinish(byte[] messageId) {
        this.inFlight.getAndDecrement();
        return Channel.super.sendFinish(messageId);
    }

    @Override
    public void close() {
        log.info("Channel closed, server: {}, current: {}",
                this.remoteAddress, getCounter(this.remoteAddress).decrementAndGet());
        this.responseListeners.clear();
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
        doSend(Commands.NOP, null);
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
        ResponseFuture responseListener = this.responseListeners.poll();
        if (responseListener != null && !responseListener.isDone()) {
            responseListener.set(response);
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

}
