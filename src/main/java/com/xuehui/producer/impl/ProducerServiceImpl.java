package com.xuehui.producer.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xuehui.producer.ProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

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
@Service
public class ProducerServiceImpl implements ProducerService {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private KafkaTemplate template;

    @Override
    public void send(String topic, String str, int count) {
        for (int i = 0; i < count; i++) {
//            template.send(topic, str);
            template.send(topic, "1", str);
        }
    }

    @Override
    public void sendJson(String topic, String json, int count) {
        for (int i = 0; i < count; i++) {
            sendJson(topic, json);
        }
    }

    private void sendJson(String topic, String json) {
        JSONObject jsonObj = JSON.parseObject(json);

        jsonObj.put("topic", topic);
        jsonObj.put("ts", System.currentTimeMillis() + "");

        logger.info("json+++++++++++++++++++++  message = {}", jsonObj.toJSONString());

        ListenableFuture<SendResult<String, String>> future = template.send(topic, jsonObj.toJSONString());
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("msg OK. " + result.toString());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("msg send failed.", ex);
            }
        });
    }
}
