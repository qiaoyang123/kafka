package com.bigdata.hbase.utils;

import com.google.common.hash.*;
import com.google.common.io.Files;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * MD5工具类
 *
 * @author <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version 1.0 2018/4/12
 * @since 1.0
 */
public class Md5Utils {
    private static HashFunction hf = Hashing.md5();
    private static Charset defaultCharset = Charset.forName("UTF-8");

    private Md5Utils() {
        throw new AssertionError("不要实例化工具类");
    }

    public static String md5(String data) {
        HashCode hash = hf.newHasher().putString(data, defaultCharset).hash();
        return hash.toString();
    }

    public static String md5(String data, Charset charset, boolean isUpperCase) {
        HashCode hash = hf.newHasher().putString(data, charset == null ? defaultCharset : charset).hash();
        return isUpperCase ? hash.toString().toUpperCase() : hash.toString();
    }

    public static String md5(byte[] bytes, boolean isUpperCase) {
        HashCode hash = hf.newHasher().putBytes(bytes).hash();
        return isUpperCase ? hash.toString().toUpperCase() : hash.toString();
    }
}
