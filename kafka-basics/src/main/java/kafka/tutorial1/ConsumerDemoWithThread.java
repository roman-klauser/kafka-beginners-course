package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

        // latch for dealing with multiple hooks
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer runnable
        logger.info("Creating consumer with thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch);

        // start the thread
        Thread thread = new Thread(myConsumerRunnable);
        thread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has existed");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Appliction got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }


    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-app");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord record: records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {
            // interrupts consumer.poll()
            // throws WakeUpException
            consumer.wakeup();
        }
    }
}
