package com.github.brainlag.nsq;

import org.junit.Test;

import java.time.LocalDateTime;

/**
 * @author hongmiao.yu
 */
public class ProducerTest {

    @Test
    public void testProduceMessage() throws InterruptedException {
        String topic = "nsq_client_test";
        Producer producer = new Producer(NsqServers.PRODUCE_SERVERS);
        while(true) {
            producer.produce(topic, ("hello " + LocalDateTime.now().toString()).getBytes());
            Thread.sleep(100L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }


}
