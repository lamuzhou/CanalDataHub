# CanalDataHub
程序主入口：canal模块 com.shdata.app.CanalClientReal -> 其中包括 1.canal监控MySQL binlog的解析代码 2.解析后发往DataHub中 DataHubSenderTest.insert())
canal模块 com.shdata.util.DataHubSenderTest中的insert方法 -> 把从canal解析出来的数据发往DataHub的对应project/topic中 其中包括 1.包装不同的DataHub topic表格类 RecordTypeByTable.recordEntriesGeneration()
canal模块 com.shdata.app.RecordTypeByTable -> 根据tablename封装不同的表格
