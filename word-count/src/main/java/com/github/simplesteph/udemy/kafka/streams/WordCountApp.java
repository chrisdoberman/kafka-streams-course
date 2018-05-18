package com.github.simplesteph.udemy.kafka.streams;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        // 1 - stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2 - map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                // can be alternatively written as:
                // .mapValues(String::toLowerCase)
                // 3 - flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurences
                .count("Counts");
        // 7 - to in order to write the results back to kafka
        wordCounts.to(Serdes.String(), Serdes.Long(),"word-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();


        // print topology
        System.out.println(streams.toString());


        // shutdown hook to correctly close the streams application
        // always do this!!!
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        /*while(true){
            System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }*/


    }
}
