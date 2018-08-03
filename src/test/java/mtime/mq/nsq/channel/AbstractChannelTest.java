package mtime.mq.nsq.channel;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

/**
 * @author hongmiao.yu
 */
public class AbstractChannelTest {

    @Test
    public void test() {
        CompletableFuture<Boolean> f = CompletableFuture.completedFuture(Boolean.TRUE);
        f.whenComplete((v, t) -> {
            System.out.println("hello");
        });
    }

}
