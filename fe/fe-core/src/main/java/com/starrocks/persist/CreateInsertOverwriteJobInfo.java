// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
public class CreateInsertOverwriteJobInfo implements Writable {
    @SerializedName(value = "jobId")
    private long jobId;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "targetPartitionIds")
    private List<Long> targetPartitionIds;

    public CreateInsertOverwriteJobInfo(long jobId, long dbId, long tableId, List<Long> targetPartitionIds) {
        this.jobId = jobId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.targetPartitionIds = targetPartitionIds;
    }

    public long getJobId() {
        return jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public List<Long> getTargetPartitionIds() {
        return targetPartitionIds;
    }

    @Override
    public String toString() {
        return "CreateInsertOverwriteJobInfo{" +
                "jobId=" + jobId +
                ", dbId=" + dbId +
                ", tableId=" + tableId +
                ", targetPartitionIds=" + targetPartitionIds +
                '}';
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
