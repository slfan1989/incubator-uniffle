package org.apache.uniffle.shuffle.resource;

import org.apache.uniffle.common.ShuffleServerInfo;

import java.util.List;

public class DefaultRssShuffleResource implements RssShuffleResource {

  private final List<ShuffleServerInfo> shuffleServerInfos;

  private int shuffleId;
  private int mapId;
  private int attemptId;
  private int partitionId;

  public DefaultRssShuffleResource(List<ShuffleServerInfo> shuffleServerInfos) {
    this.shuffleServerInfos = shuffleServerInfos;
  }

  @Override
  public List<ShuffleServerInfo> getReducePartitionLocations() {
    return shuffleServerInfos;
  }

  @Override
  public ShuffleServerInfo getMapPartitionLocation() {
    return shuffleServerInfos.get(0);
  }
}
