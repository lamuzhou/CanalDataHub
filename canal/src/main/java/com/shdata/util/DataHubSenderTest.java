package com.shdata.util;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;
import com.shdata.app.RecordTypeByTable;

import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-10-11:24 PM
 */
public class DataHubSenderTest {

    // Endpoint以Region: 华东2(上海)为例
    private static final String endpoint = "http://datahub.cn-shanghai-cyy-d01.dh.res.cloud.sh.cegn.cn";
    private static final String accessId = "UTeCgyGBdwj4Sgt0";
    private static final String accessKey = "JakRn5qGT6ZkikyffjZv7E017zmOkT";
    private static DatahubClient datahubClient;

    private static final String projectName = "ggws_project";
//    private static final String topicName = "hztj_test";
//    private static final String topicName = "tbl_org_info";
    // 可通过listShard接口获取shard列表，所有ACTIVE的shard均可使用，本例使用"0"
    private static final String shardId = "0";

    public static DatahubClient createDatahubClient() {

        // 创建DataHubClient实例
        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                //专有云使用出错尝试将参数设置为       false
                // HttpConfig可不设置，不设置时采用默认值
                .setHttpConfig(new HttpConfig().setConnTimeout(10000))
                .build();

        return client;
    }

    public static void insert(List<JSONObject> listRowData, String tableName) {
        if (datahubClient == null) {
            datahubClient = createDatahubClient();
        }
        // topic schema
//      // 一
//        RecordSchema schema = new RecordSchema();
//        schema.addField(new Field("t1",FieldType.STRING));
//        schema.addField(new Field("t2", FieldType.STRING));
        // 二
        // TUPLE类型的Topic需要设置schema，也可直接通过getTopic获取
        RecordSchema schema = datahubClient.getTopic(projectName, tableName).getRecordSchema();

        // generate 10 records
//        List<RecordEntry> recordEntries = new ArrayList<>();
//        for (int i = 0; i < 10; ++i) {
//            RecordEntry recordEntry = new RecordEntry();
//            // set attributes
//            recordEntry.addAttribute("id", "yymc");
//            // set tuple data
//            TupleRecordData data = new TupleRecordData(schema);
//            data.setField("id", i);
//            data.setField("yymc", "test " + i);
//            recordEntry.setRecordData(data);
//            recordEntries.add(recordEntry);
//        }

        //获取对应的recordEntries
        List<RecordEntry> recordEntries = RecordTypeByTable.recordEntriesGeneration(listRowData, tableName, schema);

//        for (RecordEntry recordEntry : recordEntries) {
//            RecordData data = recordEntry.getRecordData();
//            System.out.println("test: " + data);
//        }

        // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
        try {
            datahubClient.putRecordsByShard(projectName, tableName, shardId, recordEntries);
        } catch (
                InvalidParameterException e) {
            System.out.println("invalid parameter, please check your parameter");
            System.exit(1);
        } catch (
                AuthorizationFailureException e) {
            System.out.println("AK error, please check your accessId and accessKey");
            System.exit(1);
        } catch (
                ResourceNotFoundException e) {
            System.out.println("project or topic or shard not found");
            System.exit(1);
        } catch (
                ShardSealedException e) {
            System.out.println("shard status is CLOSED, can not write");
            System.exit(1);
        } catch (
                DatahubClientException e) {
            System.out.println("other error");
            System.out.println(e);
            System.exit(1);
        }
    }

//    public static void query() {
//        if (datahubClient == null) {
//            datahubClient = createDatahubClient();
//        }
//        // 数据读取
//        // 每次限读100条，最大不可超过1000
//        int recordLimit = 100;
//        // Tuple Topic schema
//        RecordSchema schema = new RecordSchema();
//        schema.addField(new Field("t1", FieldType.STRING));
//        schema.addField(new Field("t2", FieldType.STRING));
//        //获取cursor
//        // 注: 正常情况下，getCursor只需在初始化时获取一次，然后使用getRecords的nextCursor进行下一次读取
////        String cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.LATEST).getCursor();//这里获取最新的一条记录
//        String cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.SYSTEM_TIME, 1559059200000L).getCursor();
//
//        int i = 0;
//        while (true) {
//            try {
//                // 读取数据
//                GetRecordsResult result = datahubClient.getRecords(projectName, topicName, shardId, schema, cursor, recordLimit);
//                // 如果有数据则处理，无数据需sleep后重新读取
//                if (result.getRecordCount() > 0) {
//                    for (RecordEntry entry : result.getRecords()) {
//                        i++;
//                        System.out.println("i:" + i);
//                        //获取DataHub数据上传的系统时间
//                        long systemTime = entry.getSystemTime();
//                        System.out.println("systemTime:" + systemTime);
//                        TupleRecordData data = (TupleRecordData) entry.getRecordData();
//                        System.out.println("t1:" + data.getField("t1"));
//                        System.out.println("t2:" + data.getField("t2"));
//                    }
//                }
//                // 拿到下一个游标
//                cursor = result.getNextCursor();
//
//            } catch (InvalidCursorException ex) {
//                // 非法游标或游标已过期，建议重新定位后开始消费
//                cursor = datahubClient.getCursor(projectName, topicName, shardId, CursorType.OLDEST).getCursor();
//            }
//        }
//
//    }

//    public static void main(String[] args){
//        //测试数据流是否通顺
//        insert();
//        System.out.println("---------插入成功，去获取数据----------");
////        query();
//    }

}
