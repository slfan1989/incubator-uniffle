package org.apache.uniffle.flink.shuffle.rpc;

import org.apache.flink.runtime.rpc.*;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface RssShuffleRpcService extends RpcService {

  Executor getExecutor();

  <F extends Serializable, C extends RssFencedRpcGateway<F>>
     CompletableFuture<C> connectTo(String address, F fencingToken, Class<C> clazz);

  <C extends RssShuffleRpcGateway> CompletableFuture<C> connectTo(
     String address, Class<C> clazz);
}
