package kafka_first.first;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        //create producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);



        //send data;
        int i = 0;
        while(i < 10) {

            String topic = "first_topic";
            String value = "Hello World";
            String key = "id_" + Integer.toString(i);

            logger.info("Key: " + key);

            ProducerRecord<String, String> rec =
                    new ProducerRecord<String, String>( topic, key, value);
            producer.send(rec, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null)
                    {
                        logger.info("Received new metadata.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    }
                    else{
                        logger.error("Error: ", e);
                    }
                }
            });

            i++;
        }

        producer.flush();
        producer.close();

    }
}
