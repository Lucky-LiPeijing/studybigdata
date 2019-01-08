package com.lipeijing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @Classname 测试hdfs
 * @Description TODO
 * @Date 2019/1/8 12:40
 * @Created by lipeijing
 */
public class TestHdfs {

    @Test
    public void testRead() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path("hdfs://192.168.188.131:8020/lipeijing/core-site.xml");
        FSDataInputStream fis = fs.open(path);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fis, baos, 1024);
        fs.close();
        System.out.println(new String(baos.toString()));
    }

    @Test
    public void testWrite() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path("hdfs://192.168.188.131:8020/user/helloword.txt");
        FSDataOutputStream fout = fs.create(path);
        fout.write("helloworld".getBytes());
        fs.close();
    }

    /**
     * 指定副本，块大小
     * create(地址，重写，缓冲区大小，副本数，块大小<默认最小为1m>)
     * @throws IOException
     */
    @Test
    public void testWrite2() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path("hdfs://192.168.188.131:8020/user/helloword.txt");
        FSDataOutputStream fout = fs.create(path, true, 1024, (short) 2, 1048576);
        fout.write("helloworld".getBytes());
        fs.close();
    }
}
