package com.iflytek.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.ParseException;


/**
 * hbase连接单元测试
 *
 * @author yu
 */
public class HbaseDemoTest {

    /**
     * 表名
     */
    private static final String tableName = "student";

    /**
     * 列族
     */
    static String[] familys = {"grade", "course"};

    /**
     * 测试HBase表创建
     */
    @Test
    public void testCreateTable() {
        //创建表
        HbaseDemo.createTable(tableName, familys, false);
    }

    /**
     * 测试数据插入
     */
    @Test
    public void testInsertData() {
        String rowKey = "ycb";
        String colFamily = "course";
        String col = "English";
        String val = "88";

        String rowKey2 = "chinese";
        String val2 = "89";

        //插入数据
        HbaseDemo.insertData(tableName, rowKey, colFamily, col, val);

        HbaseDemo.insertData(tableName, rowKey2, colFamily, col, val2);
    }


    /**
     * 测试批量插入数据
     */
    @Test
    public void testBatchInsertData() {

        String colFamily = "grade";
        String col = "sid";
        int insertNum = 10;
        HbaseDemo.batchInsertData(tableName, colFamily, col, insertNum);
    }

    /**
     * 测试全数据扫描
     */
    @Test
    public void testGetScanData() {
        HbaseDemo.getScanData(tableName);
    }

    /**
     * 测试分页扫描数据
     */
    @Test
    public void testScanGetPage() {
        HbaseDemo.scanGetPage(tableName, 1, 2);
    }

    /**
     *
     */
    @Test
    public void testScanTimeRange() throws ParseException{
        HbaseDemo.scanTimeRange(tableName,1510123118901L,1510126004791L);
    }

    /**
     * 根据rowKey来删除该行数据
     */
    @Test
    public void testDelByRowKey() {

        String rowKey = "ycb";
        HbaseDemo.delByRowKey(tableName, rowKey);
    }


    /**
     * 根据rowKey来获取该行的整行数据
     */
    @Test
    public void testGetDataByRowKey() {
        String rowKey = "ycb";
        HbaseDemo.getDataByRowKey(tableName, rowKey);
    }

    /**
     * 测试删除列簇
     */
    @Test
    public void testDelColumnFamily() {

        byte[] colFamily = Bytes.toBytes("course");
        HbaseDemo.delColumnFamily(tableName, colFamily);
    }


    /**
     * 测试更新数据表
     */
    @Test
    public void testUpdateTable(){
        String rowKey = "ycb";
        String colFamily = "course";
        String col = "English";
        String val = "85";
        HbaseDemo.updateTable(tableName,rowKey,colFamily,col,val);
    }

}
