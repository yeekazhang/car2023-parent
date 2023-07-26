package com.atguigu.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.HBaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public abstract class MapDimFunction<T> extends RichMapFunction<T,T> {

    private Connection hBaseConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hBaseConn);
    }


    public abstract String getRowKey(T bean);

    public abstract String getTable();

    public abstract void addDims(T bean, JSONObject dim);


    @Override
    public T map(T bean) throws Exception {
        JSONObject obj = HBaseUtil.getRow(hBaseConn,"car",getTable(),getRowKey(bean));

        addDims(bean,obj);

        return bean;
    }
}
