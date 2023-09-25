package org.apache.uniffle.shuffle.resource;

import org.apache.uniffle.common.ShuffleServerInfo;

import java.io.Serializable;
import java.util.List;

public interface RssShuffleResource extends Serializable {

  List<ShuffleServerInfo> getReducePartitionLocations();

  ShuffleServerInfo getMapPartitionLocation();
}
