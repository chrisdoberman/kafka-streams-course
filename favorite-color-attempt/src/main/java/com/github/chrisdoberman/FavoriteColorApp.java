package com.github.chrisdoberman;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // disable the cache to demonstrate all the steps in the transformation, don't do this in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KStreamBuilder builder = new KStreamBuilder();

        // stream from kafka
        KStream<String, String> textLines = builder.stream("favourite-colour-input");

        KStream<String, String> usersAndColors = textLines
                // ensure a comma exists
                .filter((key, value) -> value.contains(","))
                // select the key
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // get the color from the value
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // remove bad colors
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

        // write to kafka as intermediary topic
        usersAndColors.to("user-keys-and-colours");

        // read from kafka as ktable
        KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-colours");
        // count the occurences of colors
        KTable<String, Long> favoriteColors = usersAndColorsTable
                // group by colors within the Ktable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count("CountsByColors");
        // output the results to a kafka topic
        favoriteColors.to(Serdes.String(), Serdes.Long(), "favourite-colour-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        // do a cleanup in dev mode, don't do this in prod
        streams.cleanUp();
        streams.start();

        // print topology
        System.out.println(streams.toString());

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
