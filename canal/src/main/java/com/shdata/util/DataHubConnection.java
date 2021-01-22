package com.shdata.util;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.GetProjectResult;
import com.aliyun.datahub.client.model.ListProjectResult;

import java.util.Iterator;
import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-11-11:04 AM
 */
public class DataHubConnection {

    public static void main(String[] args) {

        // Endpoint以Region: 华东1为例，其他Region请按实际情况填写
        String endpoint = "http://datahub.cn-shanghai-cyy-d01.dh.res.cloud.sh.cegn.cn";
        String accessId = "UTeCgyGBdwj4Sgt0";
        String accessKey = "JakRn5qGT6ZkikyffjZv7E017zmOkT";
        DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                //专有云使用出错尝试将参数设置为           false
                // HttpConfig可不设置，不设置时采用默认值
                .setHttpConfig(new HttpConfig()
                        .setCompressType(HttpConfig.CompressType.LZ4) // 读写数据推荐打开网络传输 LZ4压缩
                        .setConnTimeout(10000))
                .build();
//        GetProjectResult ggws_project = datahubClient.getProject("ggws_project");
//        String projectName = ggws_project.getProjectName();
//        System.out.println(projectName);

        ListProjectResult listProjectResult = datahubClient.listProject();
        List<String> projectNames = listProjectResult.getProjectNames();
        for (int i = 0; i < projectNames.size(); i++) {
            System.out.println(projectNames.get(i));
        }

    }
}
