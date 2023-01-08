package io.confluent.developer;


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerApplication {

  private volatile boolean keepConsuming = true;
  private Consumer<Integer, String> consumer;
  private String topicName = "orders";
  private Duration pto = Duration.ofSeconds(1); //Poll timeout duration

  public void runConsume(Properties props) {
    //Declare and construct Kafka Consumer instance
    
    try {
      //Subscribe to our topic
      
      while (keepConsuming) {
        //Call consumer.poll
        
        //Iterate over ConsumerRecords and display individual records.

      }
    } finally {
      consumer.close();
    }
  }

  public void shutdown() {
    keepConsuming = false;
  }

  public static Properties loadProperties(String fileName) throws IOException {
    Properties props = new Properties();
    FileInputStream input = new FileInputStream(fileName);
    props.load(input);
    input.close();
    return props;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "This program takes one argument: the path to an environment configuration file.");
    }
    Properties props = KafkaConsumerApplication.loadProperties(args[0]);
    KafkaConsumerApplication consumerApp = new KafkaConsumerApplication();

    Runtime.getRuntime().addShutdownHook(new Thread(consumerApp::shutdown));

    consumerApp.runConsume(props);
  }

}

