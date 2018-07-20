package com.github.brainlag.nsq;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;

/**
 * @author hongmiao.yu
 */
public class ProducerTest {

    @Test
    public void testProduceMessage() throws InterruptedException {
        String topic = "nsq_client_test";
        Set<ServerAddress> serverAddresses = LarkUtils.getProduceServersForTopic(topic);
        System.out.println("Servers for topic " + topic);
        System.out.println(serverAddresses);
        Producer producer = new Producer(serverAddresses);
        while(true) {
            producer.produce(topic, ("hello " + LocalDateTime.now().toString()).getBytes());
            Thread.sleep(500L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }


}
