package com.xuehui.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

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
 * <tr><td>1.0</td><td>2019/4/10</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
@Component
public class KafkaConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 监听topic1主题,单条消费
     */
    @KafkaListener(topics = "${spring.kafka.consumer.topic}")
    public void listen1(ConsumerRecord<String, String> record) {
        consumer(record);
    }

    /**
     * 监听topic,批量消费
     */
    @KafkaListener(topics = "${spring.kafka.consumer.topic}", containerFactory = "batchFactory")
//    public void listen(List<ConsumerRecord<String, String>> records) {
//        batchConsumer(records);
//    }

    /**
     * 单条消费
     */
    private void consumer(ConsumerRecord<String, String> record) {
        logger.debug("主题:{}, 内容: {}", record.topic(), record.value());

    }

    /**
     * 批量消费
     */
    private void batchConsumer(List<ConsumerRecord<String, String>> records) {
        records.forEach(record -> consumer(record));
    }
}
