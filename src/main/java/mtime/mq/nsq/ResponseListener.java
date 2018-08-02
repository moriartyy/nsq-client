package mtime.mq.nsq;

/**
 * @author hongmiao.yu
 */
public class ResponseListener {

    private final ResponseFuture responseFuture;

    public ResponseListener(ResponseFuture responseFuture) {
        this.responseFuture = responseFuture;
    }

    public void onResponse(Response response) {
        this.responseFuture.set(response);
    }
}
