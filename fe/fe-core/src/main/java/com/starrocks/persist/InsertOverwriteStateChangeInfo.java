// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.load.InsertOverwriteJob;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteStateChangeInfo implements Writable {
    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "fromState")
    private InsertOverwriteJob.OverwriteJobState fromState;
    @SerializedName(value = "toState")
    private InsertOverwriteJob.OverwriteJobState toState;
    @SerializedName(value = "sourcePartitionName")
    private List<String> sourcePartitionNames;
    @SerializedName(value = "newPartitionNames")
    private List<String> newPartitionNames;

    @SerializedName(value = "watershedTxnId")
    private long watershedTxnId;

    public InsertOverwriteStateChangeInfo(long jobId, InsertOverwriteJob.OverwriteJobState fromState,
                                          InsertOverwriteJob.OverwriteJobState toState,
                                          List<String> sourcePartitionNames, List<String> newPartitionNames,
                                          long watershedTxnId) {
        this.jobId = jobId;
        this.fromState = fromState;
        this.toState = toState;
        this.sourcePartitionNames = sourcePartitionNames;
        this.newPartitionNames = newPartitionNames;
        this.watershedTxnId = watershedTxnId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public InsertOverwriteJob.OverwriteJobState getFromState() {
        return fromState;
    }

    public void setFromState(InsertOverwriteJob.OverwriteJobState fromState) {
        this.fromState = fromState;
    }

    public InsertOverwriteJob.OverwriteJobState getToState() {
        return toState;
    }

    public void setToState(InsertOverwriteJob.OverwriteJobState toState) {
        this.toState = toState;
    }

    public List<String> getSourcePartitionNames() {
        return sourcePartitionNames;
    }

    public List<String> getNewPartitionsName() {
        return newPartitionNames;
    }

    public void setNewPartitionsName(List<String> newPartitionsName) {
        this.newPartitionNames = newPartitionsName;
    }

    public long getWatershedTxnId() {
        return watershedTxnId;
    }

    @Override
    public String toString() {
        return "InsertOverwriteStateChangeInfo{" +
                "jobId=" + jobId +
                ", fromState=" + fromState +
                ", toState=" + toState +
                ", sourcePartitionNames=" + sourcePartitionNames +
                ", newPartitionNames=" + newPartitionNames +
                ", watershedTxnId=" + watershedTxnId +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static InsertOverwriteStateChangeInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, InsertOverwriteStateChangeInfo.class);
    }
}
