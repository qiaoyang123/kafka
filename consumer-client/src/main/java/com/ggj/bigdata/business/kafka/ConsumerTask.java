package com.ggj.bigdata.business.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/29 17:50
 * @since 1.0
 */
public class ConsumerTask implements Callable {

    private ConsumerRecords<String, String> records;

    public ConsumerTask(ConsumerRecords<String, String> records){
        this.records = records;
    }

    @Override
    public Object call() throws Exception {
        Map<String,String> recordList = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String value = record.value();
            recordList.put(key,value);
        }
        return recordList;
    }
}
