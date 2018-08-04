package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.io.Inet.SocketOption
import akka.stream.OverflowStrategy

import scala.collection.immutable
import scala.concurrent.duration._

final case class PeerConfig(remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress] = None,
                            options: immutable.Seq[SocketOption] = immutable.Seq.empty,
                            halfClose: Boolean = false,
                            connectTimeout: Duration = Duration.Inf,
                            idleTimeout: Duration = Duration.Inf,
                            requestTimeout: Duration = Duration.Inf,
                            backoffConfig: Option[BackoffConfig] = None,
                            redisConnectionMode: RedisConnectionMode = RedisConnectionMode.QueueMode,
                            requestBufferSize: Int = PeerConfig.DEFAULT_MAX_REQUEST_BUFFER_SIZE,
                            overflowStrategyOnQueueMode: OverflowStrategy = OverflowStrategy.backpressure) {
  def withQueueConnectionMode: PeerConfig = copy(redisConnectionMode = RedisConnectionMode.QueueMode)
  def withActorConnectionMode: PeerConfig = copy(redisConnectionMode = RedisConnectionMode.ActorMode)
}

object PeerConfig {
  val DEFAULT_MAX_REQUEST_BUFFER_SIZE: Int = 1024
}
