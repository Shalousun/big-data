package com.iflytek.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HbaseDemo {

    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "192.168.248.145:16000");
        conf.set("hbase.zookeeper.quorum", "192.168.248.145,192.168.248.140,192.168.248.146");
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param families
     */
    public static void createTable(String tableName, String... families) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            for (String family : families) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("Table Exists");
                System.exit(0);
            } else {
                admin.createTable(tableDescriptor);
                System.out.println("Create table Success!!!Table Name:[" + tableName + "]");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param myTableName 表名
     * @param colFamily   列族
     * @param deleteFlag  删除标记
     */
    public static void createTable(String myTableName, String[] colFamily, boolean deleteFlag) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(myTableName);
            if (admin.tableExists(tableName)) {
                if (!deleteFlag) {
                    System.out.println(myTableName + " table exists!");
                } else {
                    HbaseDemo.deleteTable(myTableName);
                    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                    for (String str : colFamily) {
                        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                        hTableDescriptor.addFamily(hColumnDescriptor);
                    }
                    admin.createTable(hTableDescriptor);
                    System.out.println(myTableName + "表创建成功。。。");
                }
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String str : colFamily) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
                System.out.println(myTableName + "表创建成功。。。");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public static void deleteTable(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
            System.out.println("delete table " + tableName + " ok!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 更新表
     *
     * @param tableName  表名
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     */
    public static void updateTable(String tableName, String rowKey, String familyName, String columnName, String value) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
            System.out.println("Update table success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 往表中添加数据
     *
     * @param rowKey
     * @param tableName
     * @param column
     * @param value
     */
    public static void addData(String rowKey, String tableName, String[] column, String[] value) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
            for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString();
                if (familyName.equals("version")) {
                    for (int j = 0; j < column.length; j++) {
                        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
                    }
                    table.put(put);
                    System.out.println("Add Data Success!");
                }
            }
        } catch (IOException e) {

        }
    }


    /**
     * 根据rowKey删除所有的列
     *
     * @param tableName
     * @param rowKey
     */
    public static void delByRowKey(String tableName, String rowKey) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(tableName + " 表中rowKey为 " + rowKey + " 的数据已被删除....");
    }

    /**
     * 删除指定的列
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     */
    public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delColumn = new Delete(Bytes.toBytes(rowKey));
            delColumn.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            table.delete(delColumn);
            System.out.println("Delete Column Success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 分页扫描数据
     *
     * @param tableName
     * @param offset
     * @param limit
     */
    public static void scanGetPage(String tableName, int offset, int limit) {
        try {
            Scan scan = new Scan();
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(scan);
            int count = 0;
            for (Result r : rs) {
                if (++count <= offset) {
                    continue;
                }
                printResult(r);

                if (count == offset + limit) {
                    break;
                }

                System.out.println("----------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getResultByVersion(String tableName, String rowKey, String familyName, String columnName) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            get.setMaxVersions(3);
            Result result = table.get(get);
            for (Cell cell : result.listCells()) {
                System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println("Timestamp:" + cell.getTimestamp());
                System.out.println("---------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * @param tableName
     * @param beginTime
     * @param endTime
     * @throws ParseException
     */
    public static void scanTimeRange(String tableName, long beginTime, long endTime) throws ParseException {

        try {
            Scan scan = new Scan();
            Filter filter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator("2017-11-08 15".getBytes()));
            scan.setFilter(filter);
            scan.setReversed(true);
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(scan);

            for (Result r : rs) {
                printResult(r);
                System.out.println("----------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String nowTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }




    /**
     * 往表中添加数据(单条添加)
     *
     * @param myTableName 表名
     * @param rowKey
     * @param colFamily
     * @param col
     * @param val
     */
    public static void insertData(String myTableName, String rowKey, String colFamily, String col, String val) {

        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(myTableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
            table.put(put);
            System.out.println("数据插入成功。。。rowkey为：" + rowKey);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //close();
        }
    }

    /**
     * 往表中批量添加数据
     */

    /**
     * 往表中批量添加数据
     *
     * @param myTableName 表名
     * @param colFamily   列族
     * @param col         列
     * @param insertNum   插入行数
     */
    public static void batchInsertData(String myTableName, String colFamily, String col, int insertNum) {

        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(myTableName));
            List<Put> list = new ArrayList<>();
            Put put;
            for (int i = 0; i < insertNum; i++) {
                put = new Put(Bytes.toBytes("rowKey" + i));
                put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes("110804" + i));
                list.add(put);
            }
            table.put(list);
            System.out.println("数据插入成功。。。");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // close();
        }
    }

    /**
     * 根据表名查询整张表的数据（当然同样可根据列簇，列分割符等进行scan的查询，这里不进行细写了）
     *
     * @param tableName
     * @throws IOException
     */
    public static void getScanData(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println(new String(CellUtil.cloneRow(cell)) + "\t" + new String(CellUtil.cloneFamily(cell))
                            + "\t" + new String(CellUtil.cloneQualifier(cell)) + "\t"
                            + new String(CellUtil.cloneValue(cell)) + "\t" + cell.getTimestamp());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取数据(根据行键获取其整行数据)
     */

    /**
     * 根据rowKey来获取数据该行的整行数据
     *
     * @param myTableName 表名
     * @param rowKey      rowKey
     */
    public static void getDataByRowKey(String myTableName, String rowKey) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(myTableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result re = table.get(get);
            List<Cell> listCells = re.listCells();
            for (Cell cell : listCells) {
                System.out.println(new String(CellUtil.cloneRow(cell)) + "\t" + new String(CellUtil.cloneFamily(cell))
                        + "\t" + new String(CellUtil.cloneQualifier(cell)) + "\t"
                        + new String(CellUtil.cloneValue(cell)) + "\t" + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //close();
        }
    }

    /**
     * 删除列簇
     *
     * @param myTableName 表名
     * @param colFamily   列簇
     */
    public static void delColumnFamily(String myTableName, byte[] colFamily) {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            TableName tableName = TableName.valueOf(myTableName);
            Admin admin = connection.getAdmin();
            // 删除前先对表进行disable
            admin.deleteColumn(tableName, colFamily);
            System.out.println(tableName + " 表 " + colFamily + " 列已被删除。。。");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //
        }
    }

    /**
     * 打印测试结果
     *
     * @param rs
     */
    private static void printResult(Result rs) {
        for (Cell cell : rs.listCells()) {
            System.out.println("rowKey:" + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            System.out.println("Timestamp:" + cell.getTimestamp());
        }
    }
}
