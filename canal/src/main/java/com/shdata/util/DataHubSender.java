package com.shdata.util;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.RecordEntry;

import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-07-10:31 PM
 */
public class DataHubSender {

    // Endpoint以Region: 华东2(上海)为例
    // 优化代码时这些信息写在配置文件中
    private static final String endpoint = "https://dh-cn-shanghai.aliyuncs.com";
    private static final String accessId = "<YourAccessKeyId>";
    private static final String accessKey = "<YourAccessKeySecret>";

    private static DatahubClient datahubClient = createDatahubClient();

    public static DatahubClient createDatahubClient(){

        // 创建DataHubClient实例
        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                // 专有云使用出错尝试将参数设置为       false
                // HttpConfig可不设置，不设置时采用默认值
                .setHttpConfig(new HttpConfig().setConnTimeout(10000))
                .build();

        return client;
    }

    public static void insert(List recordEntries, String projectName, String topicName, String shardId) {

        //判断ArrayList中是否存储的是RecordEntry
        if (recordEntries != null && recordEntries.get(0) instanceof RecordEntry) {
            try {
                // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
                datahubClient.putRecordsByShard(projectName, topicName, shardId, recordEntries);
            } catch (InvalidParameterException e) {
                System.out.println("invalid parameter, please check your parameter");
                System.exit(1);
            } catch (AuthorizationFailureException e) {
                System.out.println("AK error, please check your accessId and accessKey");
                System.exit(1);
            } catch (ResourceNotFoundException e) {
                System.out.println("project or topic or shard not found");
                System.exit(1);
            } catch (ShardSealedException e) {
                System.out.println("shard status is CLOSED, can not write");
                System.exit(1);
            } catch (DatahubClientException e) {
                System.out.println("other error");
                System.out.println(e);
                System.exit(1);
            }

        }

    }

}
