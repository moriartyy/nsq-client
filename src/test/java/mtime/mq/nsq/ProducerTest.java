package mtime.mq.nsq;

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
            producer.publish(topic, message.getBytes());
            System.out.println("Sending message: " + message);
            Thread.sleep(100L);
        }
//        producer.close();

//        System.out.println("messages sent!");

    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "nsq_client_test";
        ProducerConfig config = new ProducerConfig();
        config.setLookup(NsqServers.PRODUCE_LOOKUP);
        Producer producer = new Producer(config);
        Thread t = new Thread(() -> {
            while (true) {
                String message = ("hello " + LocalDateTime.now().toString());
                producer.publish(topic, message.getBytes());
                System.out.println("Sending message: " + message);
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.setDaemon(true);
        t.start();

        Thread.sleep(1000L);
        producer.close();
    }


}
