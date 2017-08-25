package com.yuehao.consumer.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Created by yuehao on 2017/8/24.
 */
@Component
public class Receiver {

    private Gson gson = new GsonBuilder().create();

    private Logger logger = LoggerFactory.getLogger(getClass());


    //    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"0","1"}, topic = "test_01")})
    @KafkaListener(topics = "test_01")
    public void processMessage(String content, Acknowledgment ack) {
        try {
            Thread.sleep(2000);
            Message message = gson.fromJson(content, Message.class);
            logger.info("{}", gson.toJson(message));
            ack.acknowledge();
        } catch (Exception ex) {

        }
    }


    //    @KafkaListener(topicPartitions = {@TopicPartition(partitions = {"1"}, topic = "test_01")})
    @KafkaListener(topics = "test_02")
    public void processMessageTest02(String content, Acknowledgment ack) {
        try {
            Thread.sleep(2000);
            Message message = gson.fromJson(content, Message.class);
            logger.info("{}", gson.toJson(message));
            ack.acknowledge();
        } catch (Exception ex) {

        }
    }
}
