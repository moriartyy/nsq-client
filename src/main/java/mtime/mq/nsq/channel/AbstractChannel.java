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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
@Slf4j
public abstract class AbstractChannel implements Channel {
    private static final ConcurrentMap<ServerAddress, AtomicInteger> INSTANCE_COUNTERS = new ConcurrentHashMap<>();
    private final long heartbeatTimeoutMillis;
    private final BlockingDeque<ResponseListener> responseListeners;
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
        log.info("Channel created, server: {}, total: {}", remoteAddress, getCounter(remoteAddress).incrementAndGet());
    }

    private AtomicInteger getCounter(ServerAddress serverAddress) {
        return INSTANCE_COUNTERS.computeIfAbsent(serverAddress, s -> new AtomicInteger());
    }

    protected void identity(Config config) {
        try {
            Response response = this.send(Commands.identify(config)).get();
            if (!response.isOk()) {
                throw NSQExceptions.identify("Identify failed, reason: " + response.getMessage());
            }
        } catch (Exception e) {
            throw NSQExceptions.identify("Identify failed with  " + this.getRemoteAddress(), e);
        }
    }

    @Override
    public int getUnconfirmedCommands() {
        return this.responseListeners.size();
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
    public CompletableFuture<Response> send(Command command) {
        log.debug("Sending command to {}: {}", this.remoteAddress, command.getLine());
        if (command.isResponsive()) {
            return sendResponsiveCommand(command);
        } else {
            return sendResponselessCommand(command);
        }
    }

    private CompletableFuture<Response> sendResponselessCommand(Command command) {
        CompletableFuture<Response> r = new CompletableFuture<>();
        CompletableFuture<Boolean> s = doSend(command);
        s.whenComplete((v, t) -> {
            if (t != null) {
                r.completeExceptionally(t);
            } else {
                r.complete(Response.VOID);
            }
        });
        return r;
    }

    private CompletableFuture<Response> sendResponsiveCommand(Command command) {
        CompletableFuture<Response> r = new CompletableFuture<>();

        ResponseListener l = new ResponseListener(command, r, System.currentTimeMillis());
        queue(l);  // queue listener before sending command, to make sure when response returned listener is ready

        CompletableFuture<Boolean> s = doSend(command);
        s.whenComplete((v, t) -> {
            if (t != null) {
                r.completeExceptionally(t);
                l.abandon();
            }
        });
        return r;
    }

    protected abstract CompletableFuture<Boolean> doSend(Command command);

    private boolean isHeartbeatTimeout() {
        if (this.lastHeartbeatTimeMillis == 0L) {
            this.lastHeartbeatTimeMillis = System.currentTimeMillis();
            return false;
        }
        return System.currentTimeMillis() - this.lastHeartbeatTimeMillis > this.heartbeatTimeoutMillis;
    }

    private void queue(ResponseListener responseListener) {
        if (!responseListeners.offer(responseListener)) {
            throw NSQExceptions.tooManyCommand("Too many commands to " + remoteAddress + "(" + commandQueueSize + ")");
        }
        log.debug("ResponseListener queued, server:{}, command: {}, queue size:{}",
                remoteAddress, responseListener.command.getLine(), this.responseListeners.size());
    }

    @Override
    public CompletableFuture<Response> sendReady(int count) {
        return Channel.super.sendReady(count).whenComplete((v, t) -> {
            if (t == null) {
                this.readyCount = count;
            }
        });
    }

    @Override
    public CompletableFuture<Response> sendRequeue(byte[] messageId, long timeoutMS) {
        return Channel.super.sendRequeue(messageId, timeoutMS).whenComplete((v, t) -> {
            if (t == null) {
                this.inFlight.getAndDecrement();
            }
        });
    }

    @Override
    public CompletableFuture<Response> sendFinish(byte[] messageId) {
        return Channel.super.sendFinish(messageId).whenComplete((v, t) -> {
            if (t == null) {
                this.inFlight.getAndDecrement();
            }
        });
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
        send(Commands.NOP);
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
        ResponseListener l;
        while ((l = this.responseListeners.poll()) != null) {
            log.debug("Received response from {} for command {} after {}ms",
                    remoteAddress, l.command.getLine(), System.currentTimeMillis() - l.timestamp);
            if (l.isAbandoned()) {
                continue;
            }
            l.onResponse(response);
            break;
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

    class ResponseListener {
        private CompletableFuture<Response> future;
        private Command command;
        private long timestamp;
        private AtomicBoolean abandoned = new AtomicBoolean(false);

        ResponseListener(Command command, CompletableFuture<Response> future, long timestamp) {
            this.future = future;
            this.command = command;
            this.timestamp = timestamp;
        }

        public void onResponse(Response response) {
            this.future.complete(response);
        }

        public boolean isAbandoned() {
            return this.abandoned.get();
        }

        public void abandon() {
            this.abandoned.set(true);
        }
    }
}
