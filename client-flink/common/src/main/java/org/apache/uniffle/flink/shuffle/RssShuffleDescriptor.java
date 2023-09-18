package org.apache.uniffle.flink.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.uniffle.client.api.CoordinatorClient;

import java.util.List;
import java.util.Optional;

public class RssShuffleDescriptor implements ShuffleDescriptor {

  private final ResultPartitionID resultPartitionID;

  private final JobID jobId;

  // private final List<CoordinatorClient> coordinatorClients;

  public RssShuffleDescriptor(ResultPartitionID resultPartitionID, JobID jobId) {
    this.resultPartitionID = resultPartitionID;
    this.jobId = jobId;
  }

  @Override
  public ResultPartitionID getResultPartitionID() {
    return resultPartitionID;
  }

  @Override
  public Optional<ResourceID> storesLocalResourcesOn() {
    return Optional.empty();
  }

  public JobID getJobId() {
    return jobId;
  }
}
