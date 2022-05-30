// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.journal.JournalEntity;
import com.starrocks.load.InsertOverwriteJobState;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteStateChangeInfoTest {
    @Test
    public void testBasic() throws IOException {
        List<String> sourcePartitionNames = Lists.newArrayList("p1", "p2");
        List<String> newPartitionNames = Lists.newArrayList("p1_100", "p2_100");
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(100L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_RUNNING,
                sourcePartitionNames, newPartitionNames);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        stateChangeInfo.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        InsertOverwriteStateChangeInfo newStateChangeInfo = InsertOverwriteStateChangeInfo.read(dataInputStream);
        Assert.assertEquals(100L, newStateChangeInfo.getJobId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, newStateChangeInfo.getFromState());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, newStateChangeInfo.getToState());
        Assert.assertEquals(sourcePartitionNames, newStateChangeInfo.getSourcePartitionNames());
        Assert.assertEquals(newPartitionNames, newStateChangeInfo.getNewPartitionsName());

        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setOpCode(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE);
        journalEntity.setData(stateChangeInfo);
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream2 = new DataOutputStream(outputStream2);
        journalEntity.write(dataOutputStream2);

        ByteArrayInputStream inputStream2 = new ByteArrayInputStream(outputStream2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(inputStream2);
        JournalEntity journalEntity2 = new JournalEntity();
        journalEntity2.readFields(dataInputStream2);

        Assert.assertEquals(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE, journalEntity2.getOpCode());
        Assert.assertTrue(journalEntity2.getData() instanceof InsertOverwriteStateChangeInfo);
        InsertOverwriteStateChangeInfo newStateChangeInfo2 = (InsertOverwriteStateChangeInfo) journalEntity2.getData();
        Assert.assertEquals(100L, newStateChangeInfo2.getJobId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, newStateChangeInfo2.getFromState());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, newStateChangeInfo2.getToState());
        Assert.assertEquals(sourcePartitionNames, newStateChangeInfo2.getSourcePartitionNames());
        Assert.assertEquals(newPartitionNames, newStateChangeInfo2.getNewPartitionsName());
    }
}
