package com.shdata.test;

import com.aliyun.datahub.client.model.RecordEntry;
import com.shdata.util.DataHubSender;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-07-10:36 PM
 */
public class ClientTest {

    public static void main(String[] args) throws SQLException {

        //创建MySQL连接
//        Connection connection = JDBCUtil.getConnection();

        //
        while (true) {

            //抓取数据、解析数据
            String tableName = "order";

            //dummy获取到的数据
            List<RecordEntry> recordEntries = new ArrayList<>();

            //处理数据，根据类型及表名，将数据发送至DataHub
            handler(tableName, recordEntries);

        }
    }

    public static void handler(String tableName, List<RecordEntry> recordEntries) {

        if ("order".equals(tableName)) {
            //增量或者变量数据发往对应的DataHub Project和Topic
            DataHubSender.insert(recordEntries,"project01","topic01","01");
        }
        //其它table
    }

}
