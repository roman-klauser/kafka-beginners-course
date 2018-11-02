package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // 1. producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. create ProducerRecord
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello, kafka");

        // 4. send date - asynchronous
        producer.send(record);

        // flash data
        producer.flush();

        // flash and close
        producer.close();
    }
}
