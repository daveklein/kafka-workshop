package io.confluent.developer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.Random;

public class KafkaProducerApplication {

    private volatile boolean keepProducing = true;
    private String topic = "orders";

    private Map<Integer, String> loadPizzaMap(){
        Map<Integer, String> pizzas = new HashMap<Integer, String>();
        pizzas.put(1, "Prairie");
        pizzas.put(2, "Roundup");
        pizzas.put(3, "Stampede");
        pizzas.put(4, "Bronco");
        pizzas.put(5, "Sweet Swine");
        pizzas.put(6, "Texan Taco");
        return pizzas; 
    }

    public void runProducer(Properties props) {
        Producer<Integer, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        Map<Integer, String> pizzas = loadPizzaMap();
        try{
            while (keepProducing){
                Integer ix = Integer.valueOf(random.nextInt(6)) + 1;
                String val = pizzas.get(ix);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, ix, val);
                Future<RecordMetadata> metadata = producer.send(record);
                printMetadata(metadata);
            }
        }
        finally{
            producer.close();
        }
    }
    public void shutdown() {
        keepProducing = false;
    }

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties envProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public void printMetadata(Future<RecordMetadata> metadata) {
        try {
            RecordMetadata rm = metadata.get();
            System.out.println("Record written to partition " + rm.partition());
        } 
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }        
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This program requires the path to an environment configuration file as an argument.");
        }

        Properties props = KafkaProducerApplication.loadProperties(args[0]);
        KafkaProducerApplication producerApp = new KafkaProducerApplication();
        Runtime.getRuntime().addShutdownHook(new Thread(producerApp::shutdown));
        producerApp.runProducer(props);

    }
}

