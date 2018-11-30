package com.ggj.bigdata.model.hbase;

import lombok.Data;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/30 15:45
 * @since 1.0
 */
@Data
public class OdpsData {

    private String project;

    private String accessId;

    private String accessKey;

    private String endPoint;

    private String taskName;

    private String taskSql;

}
