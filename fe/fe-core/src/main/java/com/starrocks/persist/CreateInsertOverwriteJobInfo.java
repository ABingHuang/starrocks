// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateInsertOverwriteJobInfo implements Writable {
    @SerializedName(value = "jobId")
    private long jobId;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "originInsertSql")
    private String originInsertSql;

    public CreateInsertOverwriteJobInfo(long jobId, long dbId, long tableId, String originInsertSql) {
        this.jobId = jobId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.originInsertSql = originInsertSql;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getOriginInsertSql() {
        return originInsertSql;
    }

    public void setOriginInsertSql(String originInsertSql) {
        this.originInsertSql = originInsertSql;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateInsertOverwriteJobInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateInsertOverwriteJobInfo.class);
    }
}
