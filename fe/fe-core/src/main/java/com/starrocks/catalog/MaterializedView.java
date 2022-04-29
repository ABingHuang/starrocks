// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TMaterializedView;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * meta structure for materialized view
 */
public class MaterializedView extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(MaterializedView.class);

    public enum RefreshType {
        SYNC,
        ASYNC
    }

    public static class AsyncRefreshContext {
        // base table id -> (partitionid -> visible version)
        @SerializedName(value = "baseTableVisibleVersionMap")
        public Map<Long, Map<Long, Long>> baseTableVisibleVersionMap;

        public AsyncRefreshContext() {
            this.baseTableVisibleVersionMap = Maps.newHashMap();
        }

        public AsyncRefreshContext(Map<Long, Map<Long, Long>> baseTableVisibleVersionMap) {
            this.baseTableVisibleVersionMap = baseTableVisibleVersionMap;
        }

        Map<Long, Long> getPartitionVisibleVersionMapForTable(long tableId) {
            return baseTableVisibleVersionMap.get(tableId);
        }
    }

    public static class MvRefreshScheme {
        @SerializedName(value = "type")
        private RefreshType type;
        // when type is ASYNC
        // asyncRefreshContext is used to store refresh context
        @SerializedName(value = "asyncRefreshContext")
        private AsyncRefreshContext asyncRefreshContext;
        @SerializedName(value = "lastRefreshTime")
        private long lastRefreshTime;

        public MvRefreshScheme() {
            this.type = RefreshType.ASYNC;
            this.asyncRefreshContext = new AsyncRefreshContext();
            this.lastRefreshTime = 0;
        }

        public MvRefreshScheme(RefreshType type, AsyncRefreshContext asyncRefreshContext, long lastRefreshTime) {
            this.type = type;
            this.asyncRefreshContext = asyncRefreshContext;
            this.lastRefreshTime = lastRefreshTime;
        }

        public RefreshType getType() {
            return type;
        }

        public void setType(RefreshType type) {
            this.type = type;
        }

        public AsyncRefreshContext getAsyncRefreshContext() {
            return asyncRefreshContext;
        }

        public void setAsyncRefreshContext(AsyncRefreshContext asyncRefreshContext) {
            this.asyncRefreshContext = asyncRefreshContext;
        }

        public long getLastRefreshTime() {
            return lastRefreshTime;
        }

        public void setLastRefreshTime(long lastRefreshTime) {
            this.lastRefreshTime = lastRefreshTime;
        }
    }

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshScheme")
    private MvRefreshScheme refreshScheme;

    @SerializedName(value = "baseTableIds")
    private List<Long> baseTableIds;

    @SerializedName(value = "active")
    private boolean active;

    // TODO: partition expression

    // TODO: now it is original definition sql
    // for show create mv, constructing refresh job(insert into select)
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;

    public MaterializedView() {
        // for persist
        super(TableType.MATERIALIZEDVIEW);
        this.clusterId = Catalog.getCurrentCatalog().getClusterId();
        this.tableProperty = null;
        this.state = OlapTableState.NORMAL;
        this.active = true;
    }

    public MaterializedView(long id, long dbId, String mvName, List<Column> baseSchema, KeysType keysType,
                            PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                            MvRefreshScheme refreshScheme) {
        super(id, mvName, baseSchema, keysType, partitionInfo, defaultDistributionInfo,
                Catalog.getCurrentCatalog().getClusterId(), null, TableType.MATERIALIZEDVIEW);
        this.dbId = dbId;
        this.refreshScheme = refreshScheme;
        this.active = true;
    }

    public long getDbId() {
        return dbId;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getViewDefineSql() {
        return viewDefineSql;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TMaterializedView tMaterializedView = new TMaterializedView();
        tMaterializedView.setMvName(getName());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.MATERIALIZED_VIEW,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setMaterializedView(tMaterializedView);
        return tTableDescriptor;
    }

    public List<Long> getBaseTableIds() {
        return baseTableIds;
    }

    public void setBaseTableIds(List<Long> baseTableIds) {
        this.baseTableIds = baseTableIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MaterializedView that = (MaterializedView) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write other fields in gson
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MaterializedView read(DataInput in) throws IOException {
        // read other fields in gson
        String json = Text.readString(in);
        MaterializedView mv = GsonUtils.GSON.fromJson(json, MaterializedView.class);

        // In the present, the fullSchema could be rebuilt by schema change while the properties is changed by MV.
        // After that, some properties of fullSchema and nameToColumn may be not same as properties of base columns.
        // So, here we need to rebuild the fullSchema to ensure the correctness of the properties.
        mv.rebuildFullSchema();
        return mv;
    }
}
