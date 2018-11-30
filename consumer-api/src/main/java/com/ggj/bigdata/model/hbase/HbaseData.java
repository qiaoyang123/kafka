package com.ggj.bigdata.model.hbase;

import lombok.Data;

import java.util.Map;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/30 15:34
 * @since 1.0
 */
@Data
public class HbaseData {

    private String tableName;

    private String cf;

    private String qualifier;

    private Map<String,String> dataMap;
}
