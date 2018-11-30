package com.bigdata.hbase.client;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Projects;
import com.aliyun.odps.task.SQLTask;
import com.bigdata.hbase.factory.OdpsInstance;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/14 16:50
 * @since 1.0
 */
@Slf4j
public class OdpsClient {

    private String project;

    private String accessId;

    private String accessKey;

    private String endPoint;

    /**
     * @param project 项目名称
     * @param accessId accessId
     * @param accessKey accessKey
     * @param endPoint ODPS接入点
     */
    public OdpsClient(String project, String accessId, String accessKey, String endPoint) {
        this.project = project;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.endPoint = endPoint;
    }

    /**
     * 执行一个odpsSql任务
     *
     * @param sql 执行sql
     *  @param taskName 任务名称
     */
    public void excutorSqlTask(String sql, String taskName) {
        Odps odps = OdpsInstance.builder(project, accessId, accessKey, endPoint);
        Instance instance;
        try {
            instance = SQLTask.run(odps, sql, taskName, null, null);
            instance.waitForSuccess();
            SQLTask.getResultSet(instance, taskName);
        } catch (Exception e) {
            log.error("执行odps" + taskName + "异常:", e);
        }
    }

    public Projects getAllProjects() {
        Odps odps = OdpsInstance.builder(project, accessId, accessKey, endPoint);
        Projects projects = odps.projects();
        return projects;
    }

    public static void main(String[] args) {
        String sql = "CREATE TABLE test (\n" +
                "\tsummary_date bigint COMMENT '统计时间'\n" +
                ") \n" +
                "COMMENT 'test'\n" +
                " PARTITIONED BY (p_dt string COMMENT '时间分区');";

        //excutorSqlTask(sql,"test");
    }
}
