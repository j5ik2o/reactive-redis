package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.io.Inet.SocketOption
import akka.stream.OverflowStrategy

import scala.collection.immutable
import scala.concurrent.duration._

final case class PeerConfig(remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress] = None,
                            options: immutable.Seq[SocketOption] = immutable.Seq.empty,
                            halfClose: Boolean = true,
                            connectTimeout: Duration = Duration.Inf,
                            idleTimeout: Duration = Duration.Inf,
                            connectionBackoffConfig: Option[BackoffConfig] = None,
                            requestTimeout: Duration = Duration.Inf,
                            requestBackoffConfig: Option[BackoffConfig] = None,
                            requestBufferSize: Int = PeerConfig.DEFAULT_MAX_REQUEST_BUFFER_SIZE,
                            redisConnectionSourceMode: RedisConnectionSourceMode = RedisConnectionSourceMode.QueueMode,
                            overflowStrategyOnSourceQueueMode: OverflowStrategy = OverflowStrategy.backpressure) {
  def withConnectionSourceQueueMode: PeerConfig = copy(redisConnectionSourceMode = RedisConnectionSourceMode.QueueMode)
  def withConnectionSourceActorMode: PeerConfig = copy(redisConnectionSourceMode = RedisConnectionSourceMode.ActorMode)
}

object PeerConfig {
  val DEFAULT_MAX_REQUEST_BUFFER_SIZE: Int = 1024
}
