package com.example.FlinkDemooriginal;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Flinkreceiver {
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    public static FlinkKafkaConsumer011<String>
    createStringConsumerForTopic(
            String topic, String kafkaAddress) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id","kafka-streams-demo");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                topic, new SimpleStringSchema(), props);

        // Create a DataStream from the consumer
        DataStream<String> stream = env.addSource(consumer);


        // Print the consumed strings
        stream.print();

        return consumer;
    }


    public static void main(String[] args) throws Exception {
        SpringApplication.run(Flinkreceiver.class, args);
        Flinkreceiver receiver = new Flinkreceiver();
        receiver.createStringConsumerForTopic("output-topic", "kafka:9092");
          env.execute();


    }


}
