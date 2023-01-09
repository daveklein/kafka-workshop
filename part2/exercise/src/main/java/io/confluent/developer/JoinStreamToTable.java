package io.confluent.developer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.time.Duration;

import io.confluent.developer.avro.Session;
import io.confluent.developer.avro.RatedSession;
import io.confluent.developer.avro.Rating;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class JoinStreamToTable {
    final String sessionTopic = "sessions";
    final String rekeyedSessionTopic = "rekeyed-sessions";
    final String ratingTopic = "ratings";
    final String ratedSessionsTopic = "rated-sessions";

    public Topology buildTopology(Properties allProps) {
        //Declare and construct StreamsBuilder

        final SessionRatingJoiner joiner = new SessionRatingJoiner();

        //Create stream from sessions topic, assign a key and write to rekeyed-sessions topic
        
        //Create a KTable from the rekeyed-sessions topic

        //Create stream from ratings topic and assign a key
        
        //Create ratedSession stream by performing a join on ratings and sessions
        //using the SessionRatingJoiner
        
        //Write the ratedSessions stream to the rated-sessions topic

        return builder.build();
    }

    private SpecificAvroSerde<RatedSession> ratedSessionAvroSerde(Properties allProps) {
        SpecificAvroSerde<RatedSession> sessionAvroSerde = new SpecificAvroSerde<>();
        sessionAvroSerde.configure((Map)allProps, false);
        return sessionAvroSerde;
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties allProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        allProps.load(input);
        input.close();
        return allProps;
    }

    public void createTopics(Properties allProps) {
        short rf = 1;
        AdminClient client = AdminClient.create(allProps);
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(sessionTopic, 1, rf));
        topics.add(new NewTopic(rekeyedSessionTopic, 1, rf));
        topics.add(new NewTopic(ratingTopic, 1, rf));
        topics.add(new NewTopic(ratedSessionsTopic, 1, rf));
        client.createTopics(topics);
        client.close();
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        JoinStreamToTable ts = new JoinStreamToTable();
        Properties allProps = ts.loadEnvProperties(args[0]);
        allProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        allProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        Topology topology = ts.buildTopology(allProps);
        ts.createTopics(allProps);
        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

