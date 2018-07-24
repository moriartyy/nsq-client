package com.mtime.mq.nsq;

import com.mtime.mq.nsq.support.KeepAliveThread;
import org.junit.Test;

/**
 * @author hongmiao.yu
 */
public class CommandTest {
    @Test
    public void generateIdentificationBody() throws Exception {
        System.out.println(Command.generateIdentificationBody(new Config()));
        KeepAliveThread.createStarted().join();

        System.out.println("hello");
    }

}
