package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class HBaseUtil {
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop162");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(conf);

    }

    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    public static void createHBaseTable(Connection hbaseConn,
                                        String nameSpace,
                                        String table,
                                        String family) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        // 判断要建的表是否存在
        if (admin.tableExists(tableName)) {
            return;
        }
        // 列族描述器
        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
        // 表的描述器
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc) // 给表设置列族
                .build();
        admin.createTable(desc);
        admin.close();
    }

    public static void putRow(Connection conn,
                              String nameSpace,
                              String table,
                              String rowKey,
                              String family,
                              JSONObject data) throws IOException {
        // 1. 获取 table 对象
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table t = conn.getTable(tableName);

        // 2. 创建 put 对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 3. 把每列放入 put 对象中
        for (String key : data.keySet()) {
            String value = data.getString(key);
            if (value != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
            }
        }
        // 4. 向 table 对象中 put 数据
        t.put(put);

        t.close();


    }

    public static void delRow(Connection conn,
                              String nameSpace,
                              String table,
                              String rowKey) throws IOException {
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table t = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除整行
        t.delete(delete);
        t.close();
    }

    public static <T> T getRow(Connection hbaseConn,
                               String namespace,
                               String tableName,
                               String rowKey,
                               Class<T> tClass) {

        try (Table table = hbaseConn.getTable(TableName.valueOf(namespace, tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            List<Cell> cells = result.listCells();
            T t = tClass.newInstance();
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                BeanUtils.setProperty(t, key, value);
            }

            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HOST_NAME);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncHBaseConn(AsyncConnection hbaseAsyncConn) {
        if(hbaseAsyncConn != null){
            try {
                hbaseAsyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static JSONObject readDimAsync(AsyncConnection hbaseAsyncConn, String namespace, String tableName, String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hbaseAsyncConn.getTable(TableName.valueOf(namespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
















