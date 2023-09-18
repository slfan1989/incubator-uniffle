/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.flink.shuffle;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.uniffle.common.config.RssConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssResultPartitionFactory {

  private static final Logger LOG =
            LoggerFactory.getLogger(RssResultPartitionFactory.class);


    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionDeploymentDescriptor desc,
            RssConf rssConf) {
        LOG.info(
                "Create result partition -- number of buffers per result partition={}, "
                        + "number of subpartitions={}.",
                numBuffersPerPartition,
                desc.getNumberOfSubpartitions());

        return create(
                taskNameWithSubtaskAndId,
                partitionIndex,
                desc.getShuffleDescriptor().getResultPartitionID(),
                desc.getPartitionType(),
                desc.getNumberOfSubpartitions(),
                desc.getMaxParallelism(),
                createBufferPoolFactory(),
                desc.getShuffleDescriptor(),
                rssConf,
                desc.getTotalNumberOfPartitions());
    }
}
