package com.xuehui.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

import java.util.List;
import java.util.Optional;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：springboot-kafka <br>
 * 功能：<br>
 * 描述：<br>
 * 授权 : (C) Copyright (c) 2016<br>
 * 公司 : 北京博创联动科技有限公司<br>
 * ----------------------------------------------------------------------------- <br>
 * 修改历史<br>
 * <table width="432" border="1">
 * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
 * <tr><td>1.0</td><td>2019/4/25</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class MyListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String TPOIC = "xh_test";

    /**
     * id是消费者监听容器,topicPartitions是配置topic和分区,partitions是指定只接受哪个分区的数据,可以配置多个
     *
     * @param records
     */
    @KafkaListener(id = "id0", topicPartitions = {@TopicPartition(topic = TPOIC, partitions = {"0"})})
    public void listenPartition0(List<ConsumerRecord<?, ?>> records) {
        logger.info("Id0 Listener, Thread ID: " + Thread.currentThread().getId());
        logger.info("Id0 records size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            logger.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                logger.info("p0 Received message={}", message);
            }
        }
    }

    @KafkaListener(id = "id1", topicPartitions = {@TopicPartition(topic = TPOIC, partitions = {"1"})})
    public void listenPartition1(List<ConsumerRecord<?, ?>> records) {
        logger.info("Id1 Listener, Thread ID: " + Thread.currentThread().getId());
        logger.info("Id1 records size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            logger.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                logger.info("p1 Received message={}", message);
            }
        }
    }

    @KafkaListener(id = "id2", topicPartitions = {@TopicPartition(topic = TPOIC, partitions = {"2"})})
    public void listenPartition2(List<ConsumerRecord<?, ?>> records) {
        logger.info("Id2 Listener, Thread ID: " + Thread.currentThread().getId());
        logger.info("Id2 records size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            logger.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                logger.info("p2 Received message={}", message);
            }
        }
    }

    @KafkaListener(id = "id3", topicPartitions = {@TopicPartition(topic = TPOIC, partitions = {"3"})})
    public void listenPartition3(List<ConsumerRecord<?, ?>> records) {
        logger.info("Id3 Listener, Thread ID: " + Thread.currentThread().getId());
        logger.info("Id3 records size " + records.size());

        for (ConsumerRecord<?, ?> record : records) {
            Optional<?> kafkaMessage = Optional.ofNullable(record.value());
            logger.info("Received: " + record);
            if (kafkaMessage.isPresent()) {
                Object message = record.value();
                String topic = record.topic();
                logger.info("p3 Received message={}", message);
            }
        }
    }

    /**
     * 监听topic,批量消费
     *//*
    @KafkaListener(topics = TPOIC)
    public void listen(List<ConsumerRecord<String, String>> records) {
        batchConsumer(records);
    }*/

    /**
     * 单条消费
     */
    private void consumer(ConsumerRecord<String, String> record) {
        logger.info("主题:{}, 内容: {}", record.topic(), record.value());
    }

    /**
     * 批量消费
     */
    private void batchConsumer(List<ConsumerRecord<String, String>> records) {
        records.forEach(this::consumer);
    }
}
