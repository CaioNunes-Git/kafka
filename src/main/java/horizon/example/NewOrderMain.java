package horizon.example;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String,String>(properties());
        var messageValue = "1,23,25.00";
        var messageRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER",messageValue,messageValue);
        producer.send(messageRecord,(data,ex) -> {
            if(ex != null){
                ex.getStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + " partition: " + data.partition() + " /offset: " + data.offset() + " /timeStamp: " + data.timestamp());
        }).get();

    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}