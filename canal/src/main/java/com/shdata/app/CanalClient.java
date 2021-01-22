package com.shdata.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.shdata.bean.Topic;
import com.shdata.util.DataHubSenderTest;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author Zhou Fang
 * @create 2021-01-07-10:28 PM
 */
public class CanalClient {

    public static void main(String[] args) {

        String now = "";

        //1.创建Canal连接器
        //创建的是和MySql中cananl用户的连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(
                        "localhost",
                        11111),
                "example",
                "",
                "");

        while (true) {

            canalConnector.connect();
            canalConnector.subscribe("pub_test.*");
//            canalConnector.subscribe("gmall200523.*");

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
            now = dateFormat.format(new Date());

            //抓取数据
            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {

//                System.out.println("no data for now, just take a break ^-^ " + now);
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
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InstantiationException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) throws InstantiationException, IllegalAccessException {

        if ("Person".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //订单表,只需要新增数据
            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    //把RowDataList中的RowData中的一行行新插入的列按照(K,V) -> (column name, column value)的格式发到kafka
                    jsonObject.put(column.getName(), column.getValue());
                }

//                //用作测试数据延迟
//                try {
//                    Thread.sleep(new Random().nextInt(5) * 1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

                //测试获取jsonObject中的数据
                BigInteger personId = jsonObject.getBigInteger("PersonId");
                String firstName = jsonObject.getString("FirstName");

                //打印数据,并将数据发送Kafka
//                System.out.println(jsonObject);
                System.out.println("personId = " + personId + "; " + "firstName = "+ firstName);

                //发送至DataHub
//                SaveToDataHub.dataTransfer(jsonObject, Topic.class);

            }

        }

    }
}
