package com.sharpwisdom;

import com.google.common.hash.BloomFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/4.
 * 一个hbase的管理类,用来获取hbase的连接等信息
 */
public class HbaseManager {

    private static Connection connection = null;


    static{
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "172.16.72.75");
//        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
//            table = connection.getTable(TableName.valueOf("testtable"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表的方法
     * @param tableName
     */
    public static  void createTable(String tableName){
        try {
            Admin admin = connection.getAdmin();
            TableName tableName1 = TableName.valueOf(tableName);
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName1);
            HColumnDescriptor family = new HColumnDescriptor("columnFamily");//column family
            tableDescriptor.addFamily(family);
            //如果表在hbase中不存在时就创建表
            if(!admin.tableExists(tableName1))  admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表的方法
     * @param tableName
     */
    public static void deleteTable(String tableName){
        try {
            Admin admin = connection.getAdmin();
            TableName nameTable = TableName.valueOf(tableName);
            admin.disableTable(nameTable);
            admin.deleteTable(nameTable);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 向表中插入数据的方法
     * @param tableName
     */
    public static void insertData(String tableName){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put("12345678".getBytes());
            put.addColumn("columnFamily".getBytes(),null,"testFamily2".getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteData(String tableName){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete("123456789".getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void queryData(String tableName){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get("123456789".getBytes());
            Result result = table.get(get);
            System.out.println("row为:" + new String(result.getRow()) + "        column family为:" + new String(result.getValue("columnFamily".getBytes(), null)));
            //ResultScanner resultScanner = table.getScanner(new Scan());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", ".");
        //先创建表
//        createTable("test99");
        //插入数据
//        insertData("test99");
        //查询出数据
        queryData("test99");
    }
}
