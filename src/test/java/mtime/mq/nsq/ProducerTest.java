package mtime.mq.nsq;

import org.junit.Test;

import java.time.LocalDateTime;

/**
 * @author hongmiao.yu
 */
public class ProducerTest {

    @Test
    public void testProduceMessage() throws InterruptedException {
        String topic = Constants.topic;
        ProducerConfig config = new ProducerConfig();
        config.setLookup(NsqServers.PRODUCE_LOOKUP);
        Producer producer = new Producer(config);
        while (true) {
            String message = ("hello " + LocalDateTime.now().toString());
            producer.publish(topic, message.getBytes());
            System.out.println("Sending to " + topic + ": " + message);
            Thread.sleep(1000L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }

    public static void main(String[] args) throws InterruptedException {
        new ProducerTest().testProduceMessage();
    }

}
