package com.xuehui.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

//Copy from http://cloudurable.com/blog/kafka-tutorial-kafka-consumer/index.html
public class KafkaConsumerExample {
    static Logger log = LoggerFactory.getLogger(KafkaConsumerExample.class);

    //C3PRC_HADOOP(
    // "c3-hadoop-kafka10.bj:21500,c3-hadoop-kafka11.bj:21500,c3-hadoop-kafka12.bj:21500,c3-hadoop-kafka13.bj:21500,c3-hadoop-kafka14.bj:21500,c3-hadoop-kafka15.bj:21500,c3-hadoop-kafka16.bj:21500,c3-hadoop-kafka33.bj:21500,c3-hadoop-kafka34.bj:21500,c3-hadoop-kafka35.bj:21500,c3-hadoop-kafka36.bj:21500,c3-hadoop-kafka38.bj:21500,c3-hadoop-kafka39.bj:21500,c3-hadoop-kafka49.bj:21500,c3-hadoop-kafka62.bj:21500"
    // ,
    // "c3-hadoop-prc-ct01.bj:11000,c3-hadoop-prc-ct02.bj:11000,c3-hadoop-prc-ct03.bj:11000,c3-hadoop-prc-ct04.bj:11000,c3-hadoop-prc-ct05.bj:11000/kafka/c3prc-hadoop"
    // ),

    private final static String TOPIC = "kylin_demo";
    private final static String BOOTSTRAP_SERVERS = "c3-miui-data-kylin01.bj:9092";
    //"c3-miui-data-kylin01.bj:9092";

    private static ConsumerRecords<String, String> messages;
    private static Iterator<ConsumerRecord<String, String>> iterator;
    private static long watermark = 100;
    private static long numProcessedMessages = 0L;

    private static int requestTimeout = 11 * 1000;
    private static int sessionTimeout = 10 * 1000;
    private static int fetchmaxwait = 500;

    private static Consumer<String, String> createConsumer() {
        log.info("======create consumer1...");
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);


        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<String, String> consumer = createConsumer();

        final int giveUp = 100;
        int noRecordsCount = 0;

        log.info("======run consumer1...");
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                log.info("======run consumer1 noRecordsCount " + noRecordsCount);
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            log.info("======consumer1 consumerRecords size " + consumerRecords.count());
            Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
            if (!iterator.hasNext()) {
                log.info("======consumer1 No more messages, stop");
                break;
            }
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                log.info("======consumer1 record (partition:{}, offset:{}, key:{}, value:{})", record.partition(), record.offset(), record.key(), record.value());
                Thread.sleep(1000);
            }
            consumer.commitAsync();
            log.info("======consumer1 commitAsync");
            Thread.sleep(1000);
        }
        consumer.close();
        log.info("======consumer1 done...");
    }

    private static Consumer<String, String> createConsumer2() {
        log.info("======create consumer2...");
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        //Assume the kylin_demo topic has only 1 partition.
        int partion = 0;
        TopicPartition topicPartition = new TopicPartition(TOPIC, partion);
        // Subscribe to the topic.
        consumer.assign(Arrays.asList(topicPartition));
        //consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer2() throws InterruptedException, IOException {
        final Consumer<String, String> consumer = createConsumer2();

        final int giveUp = 100;
        int noRecordsCount = 0;

        log.info("======run consumer2...");

        while (true) {
            if (messages == null) {
//            log.info("{} fetching offset {} ", topic + ":" + split.getBrokers() + ":" + partition, watermark);
                TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
                consumer.seek(topicPartition, watermark);
                messages = consumer.poll(1000);
                log.info("======consumer2 messages size " + messages.count());
                iterator = messages.iterator();
                if (!iterator.hasNext()) {
                    log.info("======consumer2 No more messages, stop");
                    throw new IOException(String.format("Unexpected ending of stream, expected ending offset %d, but end at %d", 0, watermark));
                }
            }

            if (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();

                log.info("======consumer2 record (partition:{}, offset:{}, key:{}, value:{})", record.partition(), record.offset(), record.key(), record.value());
                watermark = record.offset() + 1;
                numProcessedMessages++;
                if (!iterator.hasNext()) {
                    messages = null;
                    iterator = null;
                }
                //break;
            }

            Thread.sleep(1000);
        }

        //log.info("======consumer2 done...");
    }

    public static void main(String... args) throws Exception {
        if (args != null) {
            int i = 0;
            for (String arg : args) {
                log.info("======args[{}]:{}", i, arg);
                i++;
            }

            if (args.length > 0 && args[0].equals("2")) {
                runConsumer2();
            } else {
                runConsumer();
            }
        } else {
            log.info("main args is null");
        }
    }
}
