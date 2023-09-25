package org.apache.uniffle.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.uniffle.client.api.CoordinatorClient;

import java.util.List;
import java.util.Optional;

/**
 * 1. put into ResultPartitionDeploymentDescriptor for submitting producer task
 * 2. as a known producer inside InputGateDeploymentDescriptor for submitting consumer task.
 * 3. It can contain specific partition config for ShuffleEnvironment on TE side to serve partition writer and reader.
 */
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
