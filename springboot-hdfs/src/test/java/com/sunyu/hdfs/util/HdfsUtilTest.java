package com.sunyu.hdfs.util;

import org.junit.Test;

public class HdfsUtilTest {


    @Test
    public void testCreateFileWithAppend() throws Exception{
        String str = "76266077925 2017-08-19 22:19:45 你好";
        String path = "/user/iflyrd/fujian/input/item/test.txt";
        HdfsUtil.createFileWithAppend(path,str);
//        HdfsUtil.createFile(path,str.getBytes("UTF-8"));
    }
}
