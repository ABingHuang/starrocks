package com.starrocks.catalog;

// not serialized field
// record all materialized views based on this Table
public class MvId {
    private final long dbId;
    private final long mvId;

    public MvId(long dbId, long mvId) {
        this.dbId = dbId;
        this.mvId = mvId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getMvId() {
        return mvId;
    }

    @Override
    public String toString() {
        return "MaterializedViewId{" +
                "dbId=" + dbId +
                ", mvId=" + mvId +
                '}';
    }
}
