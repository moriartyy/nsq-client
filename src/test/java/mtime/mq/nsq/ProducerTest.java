package mtime.mq.nsq;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Random;

/**
 * @author hongmiao.yu
 */
public class ProducerTest {

    @Test
    public void testProduceMessage() throws InterruptedException {
        Random random = new Random();
        String topic = "nsq_client_test";
        ProducerConfig config = new ProducerConfig();
        config.setLookup(NsqServers.PRODUCE_LOOKUP);
        Producer producer = new Producer(config);
        while (true) {
            int index = random.nextInt(1000000) % 3;
            String message = ("hello " + LocalDateTime.now().toString());
            producer.publish(topic + index, message.getBytes());
            System.out.println("Sending to " + topic + index + ": " + message);
            Thread.sleep(100L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }

    public static void main(String[] args) throws InterruptedException {
        new ProducerTest().testProduceMessage();
    }

}
