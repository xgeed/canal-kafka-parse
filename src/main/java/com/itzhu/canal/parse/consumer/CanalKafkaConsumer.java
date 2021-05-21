package com.itzhu.canal.parse.consumer;

import com.alibaba.fastjson.JSONObject;
import com.itzhu.canal.parse.bean.CanalBean;
import com.itzhu.canal.parse.parse.SqlParseFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CanalKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(CanalKafkaConsumer.class);
    @Autowired
    SqlParseFactory sqlParseFactory;

    @KafkaListener(topics = "canal-topic", groupId= "canal-consumer-group")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        String value = (String) consumerRecord.value();
        log.info("topic名称:{},key:{},分区位置:{},下标:{},value:{}",
                consumerRecord.topic(), consumerRecord.key(),consumerRecord.partition(), consumerRecord.offset(), value);
        //转换为javaBean
        CanalBean canalBean = JSONObject.parseObject(value, CanalBean.class);
        // log.info(canalBean.toString());
        List<String> sqlList = sqlParseFactory.parse(canalBean);
        for (String s : sqlList) {
            log.info(s);
        }
    }
}
