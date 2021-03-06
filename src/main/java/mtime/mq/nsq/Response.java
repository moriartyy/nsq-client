package mtime.mq.nsq;

/**
 * @author hongmiao.yu
 */
public class Response {

    public static final Response VOID = new Response(Status.OK, null);

    public enum Status {OK, ERROR}

    private final Status status;
    private final String message;

    private Response(Status status, String message) {
        this.status = status;
        this.message = message;
    }

    public boolean isOk() {
        return this.status == Status.OK;
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
