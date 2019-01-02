package com.lipeijing.archiver.test;

import org.junit.Test;

/**
 * @Classname TestConverter
 * @Description TODO
 * @Date 2019/1/2 19:55
 * @Created by lipeijing
 */
public class TestConverter {

    @Test
    public byte[] int2Bytes(int i) {
        byte[] arr = new byte[4];
        arr[0] = (byte)i;
        arr[1] = (byte)(i >> 8);
        arr[2] = (byte)(i >> 16);
        arr[3] = (byte)(i >> 24);
        return arr;
    }

    @Test
    public void bytes2Int(byte[] bytes) {
        int i = 0;

    }
}
