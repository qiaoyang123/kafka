package com.bigdata.hbase.factory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.utils.StringUtils;

/**
 * 链接odps实例
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/14 16:35
 * @since 1.0
 */
public class OdpsInstance {

    private static Odps odps;

    private OdpsInstance(){}

    public static Odps builder(String project, String accessId, String accessKey, String endPoint){
        if(odps == null){
            AliyunAccount account = new AliyunAccount(accessId,accessKey);
            odps = new Odps(account);
            odps.setEndpoint(endPoint);
            if(!StringUtils.isEmpty(project)){
                odps.setDefaultProject(project);
            }
            return odps;
        }

        return odps;
    }
}
