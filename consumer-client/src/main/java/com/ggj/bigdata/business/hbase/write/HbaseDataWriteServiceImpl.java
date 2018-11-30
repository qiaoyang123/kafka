package com.ggj.bigdata.business.hbase.write;

import com.bigdata.hbase.client.HbaseClient;
import com.ggj.bigdata.model.WriteData;
import com.ggj.bigdata.model.hbase.HbaseData;
import com.ggj.bigdata.service.DataWriteService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/30 10:27
 * @since 1.0
 */
@Service("hbaseDataWriteService")
public class HbaseDataWriteServiceImpl implements DataWriteService {

    @Value("${hbase.zkAddress}")
    private String zkAddress;

    @Override
    public void write(WriteData data) {
        HbaseData hbaseData = data.getHbaseData();
        Map<String, String> dataMap = hbaseData.getDataMap();
        HbaseClient client = new HbaseClient(zkAddress);
        client.putList(hbaseData.getTableName(),hbaseData.getCf(),hbaseData.getQualifier(),dataMap);
    }
}
