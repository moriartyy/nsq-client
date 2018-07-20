package com.github.brainlag.nsq;

/**
 * @author hongmiao.yu
 */
public class Response {

    public enum Status {OK, ERROR}

    private final Status status;
    private final String message;

    private Response(Status status, String message) {
        this.status = status;
        this.message = message;
    }

    public Status getStatus() {
        return this.status;
    }

    public String getMessage() {
        return this.message;
    }

    public static Response ok(String message) {
        return new Response(Status.OK, message);
    }

    public static Response error(String message) {
        return new Response(Status.ERROR, message);
    }

}
