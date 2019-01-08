package com.lipeijing;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * @Classname 测试hdfs
 * @Description TODO
 * @Date 2019/1/8 12:40
 * @Created by lipeijing
 */
public class TestHdfs {

    @Test
    public void testSave() throws IOException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);
    }
}
