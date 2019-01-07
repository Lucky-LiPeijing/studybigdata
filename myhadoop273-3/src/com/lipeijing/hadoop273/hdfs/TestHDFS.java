package com.lipeijing.hadoop273.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
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

    /**
     * 通过hadoop api访问文件
     */
    @Test
    public void readFileByAPI() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.188.131:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            Path p = new Path("/lipeijing/core-site.xml");
            FSDataInputStream fis = fs.open(p);
            byte[] buf = new byte[1024];
            int len = -1;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((len = fis.read(buf)) != -1) {
                baos.write(buf, 0, len);
            }
            fis.close();
            baos.close();
            System.out.println(new String(baos.toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过hadoop api访问文件
     */
    @Test
    public void readFileByAPI2() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.188.131:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            Path p = new Path("/lipeijing/core-site.xml");
            FSDataInputStream fis = fs.open(p);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copyBytes(fis, baos, 1024);
            fis.close();
            System.out.println(new String(baos.toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过hadoop api创建文件 mkdir
     */
    @Test
    public void mkdir() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.188.131:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.mkdirs(new Path("/user/lipeijing"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过hadoop api写文件 write
     */
    @Test
    public void putFile() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.188.131:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream out = fs.create(new Path("/user/lipeijing/create.txt"));
            out.write("hello".getBytes());
            out.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过hadoop api删除文件 write
     */
    @Test
    public void removeFile() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.188.131:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/user/lipeijing/"), true);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
