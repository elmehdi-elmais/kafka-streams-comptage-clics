package com.jcore;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clickStream = builder.stream("clicks");
        // Variante 1
        KTable<String, Long> totalCount = clickStream
                .selectKey((key, value) -> "total")
                .groupByKey()
                .count();

        totalCount.toStream()
                .peek((key, count) -> System.out.println("totalClicks: " + count))
                .mapValues(count -> String.valueOf(count))
                .to("click-counts", Produced.with(Serdes.String(), Serdes.String()));
            // Variant 2:
        KTable<String, Long> countByUser = clickStream
                .groupByKey()
                .count();

        countByUser.toStream()
                .peek((userId, count) -> System.out.println(userId + " : " + count))
                .mapValues(count -> String.valueOf(count))
                .to("click-counts", Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}