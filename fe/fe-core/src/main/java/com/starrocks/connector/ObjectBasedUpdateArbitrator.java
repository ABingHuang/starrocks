// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector;

import com.google.common.collect.Maps;
import com.starrocks.server.GlobalStateMgr;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// used to check whether data in object storage system has changed.
// for data in system like: s3/oss
public class ObjectBasedUpdateArbitrator extends TableUpdateArbitrator {

    @Override
    public Map<String, Optional<PartitionDataInfo>> getPartitionDataInfos() {
        Map<String, Optional<PartitionDataInfo>> partitionDataInfos = Maps.newHashMap();
        List<String> partitionNameToFetch = partitionNames;
        if (partitionLimit >= 0) {
            partitionNameToFetch = partitionNames.subList(0, partitionLimit);
        }
        List<RemoteFileInfo> remoteFileInfos =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(table, partitionNameToFetch);
        for (int i = 0; i < partitionNameToFetch.size(); i++) {
            RemoteFileInfo remoteFileInfo = remoteFileInfos.get(0);
            List<RemoteFileDesc> remoteFileDescs = remoteFileInfo.getFiles();
            if (remoteFileDescs != null) {
                long lastFileModifiedTime = Long.MIN_VALUE;
                int fileNumber = remoteFileDescs.size();
                Optional<RemoteFileDesc> maxLastModifiedTimeFile = remoteFileDescs.stream()
                        .max(Comparator.comparingLong(RemoteFileDesc::getModificationTime));
                if (maxLastModifiedTimeFile.isPresent()) {
                    lastFileModifiedTime = maxLastModifiedTimeFile.get().getModificationTime();
                }
                PartitionDataInfo partitionDataInfo = new PartitionDataInfo(lastFileModifiedTime, fileNumber);
                partitionDataInfos.put(partitionNameToFetch.get(i), Optional.of(partitionDataInfo));
            } else {
                partitionDataInfos.put(partitionNameToFetch.get(i), Optional.empty());
            }
        }
        return partitionDataInfos;
    }




}
