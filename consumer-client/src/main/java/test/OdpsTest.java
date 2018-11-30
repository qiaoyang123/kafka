package test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Projects;
import com.bigdata.hbase.client.HbaseClient;
import com.bigdata.hbase.client.OdpsClient;
import com.ggj.bigdata.ServerBootApplication;
import com.ggj.bigdata.business.hbase.write.HbaseDataWriteServiceImpl;
import com.ggj.bigdata.model.WriteData;
import com.ggj.bigdata.model.hbase.HbaseData;
import com.ggj.bigdata.service.DataWriteService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/14 17:09
 * @since 1.0
 */
@SpringBootTest(classes = ServerBootApplication.class)
@RunWith(SpringRunner.class)
public class OdpsTest {


    @Value("${hbase.zkAddress}")
    private String zkAddress;

    @Value("${odps.access_id}")
    private String accessId;

    @Value("${odps.access_key}")
    private String accessKey;

    @Value("${odps.endpoint}")
    private String endpoint;

    @Value("${odps.project}")
    private String project;

    @Resource(name = "hbaseDataWriteServiceImpl")
    private DataWriteService writeService;

    @Test
    public void testSqlTask() {
        String sql = "CREATE TABLE test (\n" +
                "\tsummary_date bigint COMMENT '统计时间'\n" +
                ") \n" +
                "COMMENT 'test'\n" +
                " PARTITIONED BY (p_dt string COMMENT '时间分区');";

        OdpsClient client = new OdpsClient(project,accessId,accessKey,endpoint);
        client.excutorSqlTask(sql, "test");
    }

    @Test
    public void testProjects() throws OdpsException {
        OdpsClient client = new OdpsClient(project,accessId,accessKey,endpoint);
        Projects allProjects = client.getAllProjects();
        System.out.println(allProjects.get().getName());
        System.out.println(allProjects);
    }

    @Test
    public void testCreate() {
        HbaseClient client = new HbaseClient(zkAddress);
        client.create("test", "cf");
    }

    @Test
    public void testGetTable() {
        HbaseClient client = new HbaseClient(zkAddress);
        Table table = client.getTable("test");
        TableName name = table.getName();
        System.out.println("hbase表名：" + name);
    }

    @Test
    public void testWriteHbase(){
        Map<String,String> dataMap = new HashMap<>();
        dataMap.put("123_test","test");
        WriteData data = new WriteData();
        HbaseData hbaseData = new HbaseData();
        hbaseData.setTableName("test");
        hbaseData.setCf("cf");
        hbaseData.setQualifier("test");
        hbaseData.setDataMap(dataMap);
        data.setHbaseData(hbaseData);
        writeService.write(data);
    }
}
