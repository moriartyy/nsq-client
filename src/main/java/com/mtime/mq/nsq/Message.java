package com.mtime.mq.nsq;

import com.mtime.mq.nsq.channel.Channel;

import java.util.Date;

public class Message {
    private byte[] id;
    private int attempts;
    private Date timestamp;
    private byte[] message;
    private boolean handled;
    private Date receiveTime;
    private Channel channel;

    /**
     * Finished processing this message, let nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        if (!this.handled) {
            channel.sendFinish(this.id);
            this.handled = true;
        }
    }

    public void touch() {
        channel.sendTouch(this.id);
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        if (!this.handled) {
            channel.sendRequeue(this.id, timeoutMillis);
            this.handled = true;
        }
    }

    public void requeue() {
        requeue(0);
    }

    public byte[] getId() {
        return id;
    }

    public void setId(byte[] id) {
        this.id = id;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public Date getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(Date receiveTime) {
        this.receiveTime = receiveTime;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }
}
