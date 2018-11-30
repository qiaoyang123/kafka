package com.bigdata.hbase.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * row key 获取工具类
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/28 15:23
 * @since 1.0
 */
public class RowKeyUtils {

    /**
     * 保证数据的均匀分布，保证一定的有序性
     * @param key
     * @return
     */
    public static String getHashRowKey(String key){
        return Md5Utils.md5(key).substring(0,4) + "-" + "key";
    }


    /**
     * 保证数据的均匀分布，牺牲了有序性
     * @param key
     * @return
     */
    public static String getRandomCharRowKey(String key){
        String randomStr = RandomStringUtils.random(4);
        return randomStr + "-" + key;
    }

    /**
     * 加一个变化的字符串的反转做前缀，根据prefix决定效果
     * @param key
     * @param prefix
     * @return
     */
    public static String getReversePrefixRowKey(String key,String prefix){
        return StringUtils.reverse(prefix) + "-" + key;
    }
}
