// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.starrocks.analysis.InsertStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import jersey.repackaged.com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

public class InsertOverwriteJobManagerTest {
    @Mocked
    private ConnectContext context;

    @Mocked
    private StmtExecutor stmtExecutor;

    @Mocked
    private InsertStmt insertStmt;

    @Mocked
    private Database db;

    @Mocked
    private OlapTable table1;

    private InsertOverwriteJobManager insertOverwriteJobManager;
    private InsertOverwriteJob insertOverwriteJob1;
    private Set<Long> targetPartitionIds;

    @Before
    public void setUp() {
        insertOverwriteJobManager = new InsertOverwriteJobManager();
        targetPartitionIds = Sets.newHashSet(10L, 20L, 30L, 40L);
    }

    @Test
    public void testSerialization() throws IOException {
        new Expectations() {
            {
                db.getId();
                result = 100L;

                table1.getId();
                result = 110L;

                table1.getName();
                result = "table_1";
            }
        };
        insertOverwriteJob1 =
                new InsertOverwriteJob(1000L, context, stmtExecutor, insertStmt, db, table1, targetPartitionIds);

        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob1);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        insertOverwriteJobManager.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        InsertOverwriteJobManager newInsertOverwriteJobManager = InsertOverwriteJobManager.read(dataInputStream);
        Assert.assertEquals(1, newInsertOverwriteJobManager.getJobNum());
    }
}
