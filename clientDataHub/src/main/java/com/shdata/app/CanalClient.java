package com.shdata.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-15-3:03 PM
 */
public class CanalClient {

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
//        datahubClient = DataHubSender.createDatahubClient();

        while (true) {

//            //检查dataHub连接是否还alive
//            if (datahubClient == null) {
//                datahubClient = DataHubSender.createDatahubClient();
//            }

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
//                            handler(tableName, eventType, rowDatasList, datahubClient);
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

        if ("tbl_org_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

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
//                System.out.println(jsonObject);

            }

            //插入数据
//            DataHubSenderTest.insert(listRowData, "tbl_org_info");

        }

    }

}
