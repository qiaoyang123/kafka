package com.bigdata.hbase.factory;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * hbase链接实例
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/27 16:51
 * @since 1.0
 */
@Slf4j
public class HbaseInstance {

    private String zkAddress;

    public HbaseInstance(String zkAddress){
        this.zkAddress = zkAddress;
    }

    public Connection getConnection(){
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            log.error("Hbase创建连接失败",e.getMessage());
            return null;
        }
        return connection;
    }
}
