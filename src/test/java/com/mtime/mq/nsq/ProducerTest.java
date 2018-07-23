package com.mtime.mq.nsq;

import org.junit.Test;

import java.time.LocalDateTime;

/**
 * @author hongmiao.yu
 */
public class ProducerTest {

    @Test
    public void testProduceMessage() throws InterruptedException {
        String topic = "nsq_client_test";
        ProducerConfig config = new ProducerConfig();
        config.setLookup(NsqServers.PRODUCE_LOOKUP);
        Producer producer = new Producer(config);
        while (true) {
            String message = ("hello " + LocalDateTime.now().toString());
            producer.produce(topic, message.getBytes());
            System.out.println("Sending message: " + message);
            Thread.sleep(100L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }


}
