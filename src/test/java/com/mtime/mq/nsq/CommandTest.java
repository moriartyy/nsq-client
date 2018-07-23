package com.mtime.mq.nsq;

import org.junit.Test;

/**
 * @author hongmiao.yu
 */
public class CommandTest {
    @Test
    public void generateIdentificationBody() throws Exception {
        System.out.println(Command.generateIdentificationBody(new Config()));
    }

}
