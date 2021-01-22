package com.shdata.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.RecordSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.shdata.util.DataHubSender;
import com.shdata.util.DataHubSenderTest;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.junit.Test;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-07-10:28 PM
 */
public class CanalClientReal {

    //建立DataHub连接
//    public static DatahubClient datahubClient;

    public static void main(String[] args) {

        //1.创建Canal连接器
        //创建的是和MySql中cananl用户的连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(
                        "localhost",
                        11111),
                "example",
                "",
                "");

        //创建各种连接
        canalConnector.connect();
        canalConnector.subscribe("covid19_check.*");

        while (true) {

            //检查Canal连接是否还alive
            if (canalConnector == null) {
                canalConnector.connect();
                canalConnector.subscribe("covid19_check.*");
            }

            //抓取数据
            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //解析message
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断类型，如果为非ROWDATA类型，则不进行解析
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        try {
                            //获取表名
                            String tableName = entry.getHeader().getTableName();
                            //获取数据
                            ByteString storeValue = entry.getStoreValue();
                            //反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //获取操作数据类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //获取数据集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //处理数据,根据类型及表名,将数据发送至Kafka
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //获取RecordEntries的集合
        List<JSONObject> listRowData = sender(rowDatasList);

        if ("tbl_org_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //插入数据到
            DataHubSenderTest.insert(listRowData, tableName);
        } else {
            //DataHub的其它两张表后面加了_1
            DataHubSenderTest.insert(listRowData, tableName + "_1");
        }

    }

    public static List<JSONObject> sender(List<CanalEntry.RowData> rowDatasList) {

        List<JSONObject> listRowData = new ArrayList<>();

        //订单表,只需要新增数据
        for (CanalEntry.RowData rowData : rowDatasList) {

            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                //把RowDataList中的RowData中的一行行新插入的列按照(K,V) -> (column name, column value)的格式发到kafka
                jsonObject.put(column.getName(), column.getValue());
            }

            //添加jsonObject进入listRowData生成RecordEntries
            listRowData.add(jsonObject);
//            System.out.println(jsonObject);

        }

        return listRowData;

    }

}
