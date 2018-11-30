package com.bigdata.hbase.client;


import com.bigdata.hbase.factory.HbaseInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Hbase消费Kafka数据，写入数据
 *
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/27 16:46
 * @since 1.0
 */
@Slf4j
public class HbaseClient {

    private String zkAddress;

    public HbaseClient(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public void create(String tableName, String cf) {
        Connection connection = getConnection();
        if (connection == null) {
            log.error("获取Hbase连接失败");
            return;
        }

        try {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
            Admin admin = connection.getAdmin();
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            log.error("Hbase创建表" + tableName + "失败", e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    log.error("创建表" + tableName + "关闭Hbase链接异常", e);
                }
            }
        }

    }

    public Table getTable(String tableName) {
        Connection connection = getConnection();
        if (connection == null) {
            log.error("获取Hbase连接失败");
            return null;
        }

        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("Hbase获取表" + tableName + "失败", e);
            return null;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    log.error("获取表" + tableName + "关闭Hbase链接异常", e);
                }
            }
        }
        return table;
    }

    public void put(String tableName, String cf, String qualifier, String rowKey, String value) {
        Table table = getTable(tableName);
        if (table == null) {
            log.error("hbase存入" + tableName + "获取表异常");
        }

        Put put = new Put(rowKey.getBytes());
        put.addColumn(cf.getBytes(), qualifier.getBytes(), value.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("存入数据" + tableName + "关闭Hbase链接异常", e);
                }
            }
        }

    }

    public void putList(String tableName, String cf, String qualifier, Map<String, String> data) {
        Table table = getTable(tableName);
        if (table == null) {
            log.error("hbase存入" + tableName + "List,获取表异常");
        }

        List<Put> putList = new ArrayList<>();

        Iterator<Map.Entry<String, String>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            String rowKey = next.getKey();
            String value = next.getValue();
            Put put = new Put(rowKey.getBytes());
            put.addColumn(cf.getBytes(), qualifier.getBytes(), value.getBytes());
        }

        try {
            table.put(putList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("存入数据List" + tableName + "关闭Hbase链接异常", e);
                }
            }
        }

    }

    private Connection getConnection() {
        HbaseInstance instance = new HbaseInstance(zkAddress);
        Connection connection = instance.getConnection();
        return connection;
    }

}
