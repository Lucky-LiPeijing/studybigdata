package com.lipeijing.hadoop273.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * @Classname TestHDFS
 * @Description 完成hdfs操作
 * @Date 2019/1/7 12:43
 * @Created by lipeijing
 */
public class TestHDFS {
    /**
     * 读取hdfs文件
     */
    @Test
    public void readFile() {
        try {
            // 注册URL流处理器工厂（hdfs）
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
            URL url = new URL("hdfs://192.168.188.131:8020/lipeijing/core-site.xml");
            URLConnection conn = url.openConnection();
            InputStream is = conn.getInputStream();
            byte[] buf = new byte[is.available()];
            is.read(buf);
            is.close();
            String str = new String(buf);
            System.out.println(str);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
