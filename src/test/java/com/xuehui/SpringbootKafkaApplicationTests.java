package com.xuehui;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootKafkaApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Test
    public void t1() {
        Properties props = new Properties();
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //初始化KafkaUtil
        /*KafkaOffsetUtil kafkaClusterUtil = new KafkaOffsetUtil(
                props.getProperty("spring.kafka.bootstrap-servers"),
                props.getProperty("spring.kafka.consumer.group-id"),
                Arrays.asList(props.getProperty("spring.kafka.consumer.topic").split(",")));

        kafkaClusterUtil.progress();*/
    }

}
