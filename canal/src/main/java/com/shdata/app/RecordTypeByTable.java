package com.shdata.app;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhou Fang
 * @create 2021-01-11-3:38 PM
 */
public class RecordTypeByTable {

    public static List<RecordEntry> recordEntriesGeneration(List<JSONObject> listRowData, String tableName, RecordSchema schema) {

        List<RecordEntry> recordEntries = new ArrayList<>();

        if (tableName.equals("tbl_org_info")) {

            for (int i = 0; i < listRowData.size(); i++) {

                RecordEntry recordEntry = new RecordEntry();
                // set attributes
//                recordEntry.addAttribute("key1", "value1");
                // set tuple data
                TupleRecordData data = new TupleRecordData(schema);
                data.setField("id", listRowData.get(i).getInteger("id"));
                data.setField("org_type", listRowData.get(i).getInteger("org_type"));
                data.setField("org_code", listRowData.get(i).getString("org_code"));
                data.setField("org_name", listRowData.get(i).getString("org_name"));
                data.setField("region_code", listRowData.get(i).getString("region_code"));
                data.setField("region_name", listRowData.get(i).getString("region_name"));
                data.setField("manage_org", listRowData.get(i).getString("manage_org"));
                data.setField("org_mold", listRowData.get(i).getString("org_mold"));
                data.setField("business", listRowData.get(i).getString("business"));
                data.setField("api_permission", listRowData.get(i).getInteger("api_permission"));
                recordEntry.setRecordData(data);
                recordEntries.add(recordEntry);

            }

        } else if (tableName.equals("tbl_check_summary_1")) {

            for (int i = 0; i < listRowData.size(); i++) {

                RecordEntry recordEntry = new RecordEntry();
                TupleRecordData data = new TupleRecordData(schema);
                data.setField("sample_code", listRowData.get(i).getString("sample_code"));
                data.setField("receive_org_code", listRowData.get(i).getString("receive_org_code"));
//                data.setField("receive_time", Timestamp.valueOf(listRowData.get(i).getString("receive_time")));
                data.setField("receive_time", listRowData.get(i).getString("receive_time"));
                data.setField("check_type", listRowData.get(i).getInteger("check_type"));
                data.setField("mix_id", listRowData.get(i).getString("mix_id"));
                data.setField("nuclein_check_result", listRowData.get(i).getInteger("nuclein_check_result"));
                data.setField("antibody_check_result", listRowData.get(i).getInteger("antibody_check_result"));
                data.setField("status", listRowData.get(i).getInteger("status"));
//                data.setField("report_entry_time", Timestamp.valueOf(listRowData.get(i).getString("report_entry_time")));
                data.setField("report_entry_time", listRowData.get(i).getString("report_entry_time"));
                data.setField("operator", listRowData.get(i).getString("operator"));
                data.setField("reviewer", listRowData.get(i).getString("reviewer"));
                data.setField("remarks", listRowData.get(i).getString("remarks"));
                data.setField("data_from", listRowData.get(i).getInteger("data_from"));
//                data.setField("create_time", Timestamp.valueOf(listRowData.get(i).getString("create_time")));
                data.setField("create_time", listRowData.get(i).getString("create_time"));
//                data.setField("update_time", Timestamp.valueOf(listRowData.get(i).getString("update_time")));
                data.setField("update_time", listRowData.get(i).getString("update_time"));

                recordEntry.setRecordData(data);
                recordEntries.add(recordEntry);

            }

        } else if (tableName.equals("tbl_sample_info_1")) {

            for (int i = 0; i < listRowData.size(); i++) {

                RecordEntry recordEntry = new RecordEntry();
                TupleRecordData data = new TupleRecordData(schema);
                data.setField("id", listRowData.get(i).getInteger("id"));
                data.setField("sample_org_name", listRowData.get(i).getString("sample_org_name"));
                data.setField("sample_org_code", listRowData.get(i).getString("sample_org_code"));
                data.setField("sample_operator", listRowData.get(i).getString("sample_operator"));
                data.setField("sample_time", listRowData.get(i).getString("sample_time"));
                data.setField("sample_code", listRowData.get(i).getString("sample_code"));
                data.setField("print_time", listRowData.get(i).getString("print_time"));
                data.setField("collect_mode", listRowData.get(i).getInteger("collect_mode"));
                data.setField("sample_type", listRowData.get(i).getInteger("sample_type"));
                data.setField("sample_target", listRowData.get(i).getInteger("sample_target"));
                data.setField("account_id", listRowData.get(i).getString("account_id"));
                data.setField("reg_code", listRowData.get(i).getInteger("reg_code"));
                data.setField("real_name", listRowData.get(i).getString("real_name"));
                data.setField("gender", listRowData.get(i).getInteger("gender"));
                data.setField("age", listRowData.get(i).getInteger("age"));
                data.setField("card_type", listRowData.get(i).getString("card_type"));
                data.setField("card_no", listRowData.get(i).getString("card_no"));
                data.setField("mobile", listRowData.get(i).getString("mobile"));
                data.setField("address", listRowData.get(i).getString("address"));
                data.setField("company", listRowData.get(i).getString("company"));
                data.setField("reg_status", listRowData.get(i).getInteger("reg_status"));
                data.setField("sample_station_name", listRowData.get(i).getString("sample_station_name"));
                data.setField("appointment_time", listRowData.get(i).getString("appointment_time"));
                data.setField("sample_project", listRowData.get(i).getString("sample_project"));
                data.setField("pay_status", listRowData.get(i).getString("pay_status"));
                data.setField("deleted", listRowData.get(i).getInteger("deleted"));
                data.setField("create_time", listRowData.get(i).getString("create_time"));
                data.setField("create_by", listRowData.get(i).getString("create_by"));
                data.setField("update_time", listRowData.get(i).getString("update_time"));
                data.setField("update_by", listRowData.get(i).getString("update_by"));

                recordEntry.setRecordData(data);
                recordEntries.add(recordEntry);

            }

        }

        return recordEntries;

    }
}
