package com.yuehao.consumer.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * Created by yuehao on 2017/8/24.
 */
@Component
public class Receiver {

    private Gson gson = new GsonBuilder().create();

    private Logger logger = LoggerFactory.getLogger(getClass());


    //    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"0","1"}, topic = "test_01")})
//    @KafkaListener(topics = "test_01")
//    public void processMessage(String content, Acknowledgment ack) {
//        try {
//
//            Thread.sleep(2000);
//            Message message = gson.fromJson(content, Message.class);
//            logger.info("{}", "接受到test_01的数据，开始解析...");
//            logger.info("{}", gson.toJson(message));
//            ack.acknowledge();
//        } catch (Exception ex) {
//
//        }
//    }

//    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"0","1"}, topic = "test_01")})
//    @KafkaListener(topics = "test_02")
//    public void processMessageTest02(String content, Acknowledgment ack) {
//        try {
//            Thread.sleep(2000);
//            Message message = gson.fromJson(content, Message.class);
//            logger.info("{}", "接受到test_02的数据，开始解析...");
//            logger.info("{}", gson.toJson(message));
//            ack.acknowledge();
//        } catch (Exception ex) {
//
//        }
//    }

    //    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"0", "1"}, topic = "test_01")})
//    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"0", "1"}, topic = "test_02")})

    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"0", "1", "2", "3"}, topic = "test_02")}, topics = "test_02")
//    @KafkaListener(topics = "test_02")
    public void message(ConsumerRecord data) {
        System.out.println(data.key());
        System.out.println(data.partition());
        System.out.println(gson.toJson(data.value()));
    }
}
