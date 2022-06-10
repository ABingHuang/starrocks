// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteJobManagerTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private EditLog editLog;

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

    @Mocked
    private InsertOverwriteJobRunner runner;

    private InsertOverwriteJobManager insertOverwriteJobManager;
    private List<Long> targetPartitionIds;

    @Before
    public void setUp() {
        insertOverwriteJobManager = new InsertOverwriteJobManager();
        targetPartitionIds = Lists.newArrayList(10L, 20L, 30L, 40L);
    }

    @Test
    public void testBasic() throws Exception {
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(1100L, 100L, 110L, targetPartitionIds);

        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());

        InsertOverwriteJob job2 = insertOverwriteJobManager.getInsertOverwriteJob(1100L);
        Assert.assertEquals(1100L, job2.getJobId());
        Assert.assertEquals(100L, job2.getTargetDbId());
        Assert.assertEquals(110L, job2.getTargetTableId());
        Assert.assertEquals(targetPartitionIds, job2.getOriginalTargetPartitionIds());

        insertOverwriteJobManager.deregisterOverwriteJob(1100L);
        Assert.assertEquals(0, insertOverwriteJobManager.getJobNum());

        insertOverwriteJobManager.submitJob(context, stmtExecutor, insertOverwriteJob);

        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());
    }

    @Test
    public void testReplay() throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getServingState();
                result = globalStateMgr;

                GlobalStateMgr.isCheckpointThread();
                result = false;

                globalStateMgr.getServingState();
                result = globalStateMgr;

                globalStateMgr.isReady();
                result = true;
            }
        };

        CreateInsertOverwriteJobInfo jobInfo = new CreateInsertOverwriteJobInfo(
                1100L, 100L, 110L, targetPartitionIds);
        Assert.assertEquals(1100L, jobInfo.getJobId());
        Assert.assertEquals(100L, jobInfo.getDbId());
        Assert.assertEquals(110L, jobInfo.getTableId());
        Assert.assertEquals(targetPartitionIds, jobInfo.getTargetPartitionIds());

        insertOverwriteJobManager.replayCreateInsertOverwrite(jobInfo);
        Assert.assertEquals(1, insertOverwriteJobManager.getRunningJobSize());
        insertOverwriteJobManager.cancelRunningJobs();
        Thread.sleep(5000);
        Assert.assertEquals(0, insertOverwriteJobManager.getRunningJobSize());

        insertOverwriteJobManager.replayCreateInsertOverwrite(jobInfo);
        Assert.assertEquals(1, insertOverwriteJobManager.getRunningJobSize());
        List<String> sourcePartitionNames = Lists.newArrayList("p1");
        List<String> newPartitionNames = Lists.newArrayList("p1_1100L");
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(1100L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_RUNNING,
                sourcePartitionNames, newPartitionNames);
        insertOverwriteJobManager.replayInsertOverwriteStateChange(stateChangeInfo);

        InsertOverwriteStateChangeInfo stateChangeInfo2 = new InsertOverwriteStateChangeInfo(1100L,
                InsertOverwriteJobState.OVERWRITE_RUNNING, InsertOverwriteJobState.OVERWRITE_SUCCESS,
                sourcePartitionNames, newPartitionNames);
        insertOverwriteJobManager.replayInsertOverwriteStateChange(stateChangeInfo2);
    }

    @Test
    public void testSerialization() throws IOException {
        InsertOverwriteJob insertOverwriteJob1 = new InsertOverwriteJob(1000L, 100L, 110L, targetPartitionIds);
        insertOverwriteJobManager.registerOverwriteJob(insertOverwriteJob1);
        Assert.assertEquals(1, insertOverwriteJobManager.getJobNum());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        insertOverwriteJobManager.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        InsertOverwriteJobManager newInsertOverwriteJobManager = InsertOverwriteJobManager.read(dataInputStream);
        Assert.assertEquals(1, newInsertOverwriteJobManager.getJobNum());
        InsertOverwriteJob newJob = insertOverwriteJobManager.getInsertOverwriteJob(1000L);
        Assert.assertEquals(insertOverwriteJob1, newJob);
        Assert.assertEquals(1000L, newJob.getJobId());
        Assert.assertEquals(100L, newJob.getTargetDbId());
        Assert.assertEquals(110L, newJob.getTargetTableId());
        Assert.assertEquals(targetPartitionIds, newJob.getOriginalTargetPartitionIds());
    }
}
