package com.ggj.bigdata.business.hbase;

import com.ggj.bigdata.business.hbase.write.HbaseDataWriteServiceImpl;
import com.ggj.bigdata.business.kafka.ConsumerTask;
import com.ggj.bigdata.comsumer.KafkaConsumerClient;
import com.ggj.bigdata.model.WriteData;
import com.ggj.bigdata.model.hbase.HbaseData;
import com.ggj.bigdata.service.DataWriteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/28 17:34
 * @since 1.0
 */
@Service
@Slf4j
public class HbaseBusiness {

    @Value("${kafka.topics}")
    private String topics;

    @Value("${kafka.groupId}")
    private String groupId;

    @Resource(name = "hbaseDataWriteService")
    private DataWriteService writeService;


    private ExecutorService executor;


    public void kafkaToHbase(){

        String[] topicArr = topics.split(",");

        KafkaConsumer consumer = KafkaConsumerClient.getConsumer(topicArr, groupId);
        executor = Executors.newFixedThreadPool(10);

        //循环消费消息
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                //必须在下次poll之前消费完这些数据, 且总耗时不得超过SESSION_TIMEOUT_MS_CONFIG
                //建议开一个单独的线程池来消费消息，然后异步返回结果
                Future submit = executor.submit(new ConsumerTask(records));
                while (submit.isDone()){
                    Map<String,String> recordList = (Map<String, String>) submit.get();
                    WriteData data = new WriteData();
                    HbaseData hbaseData = new HbaseData();
                    hbaseData.setDataMap(recordList);
                    //todo 设置表明
                    //todo 设置列蔟
                    //todo 设置列名
                    writeService.write(data);
                    break;
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (Throwable ignore) {
                    log.error("Thread sleep failure",e.getMessage());
                }
                throw new RuntimeException("读取kafka消息队列失败",e);
            }
        }
    }
}
