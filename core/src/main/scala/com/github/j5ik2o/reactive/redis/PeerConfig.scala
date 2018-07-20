package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.io.Inet.SocketOption
import akka.stream.OverflowStrategy

import scala.collection.immutable
import scala.concurrent.duration._

case class BackoffConfig(minBackoff: FiniteDuration = 3 seconds,
                         maxBackoff: FiniteDuration = 30 seconds,
                         randomFactor: Double = 0.2,
                         maxRestarts: Int = -1)

case class PeerConfig(remoteAddress: InetSocketAddress,
                      localAddress: Option[InetSocketAddress] = None,
                      options: immutable.Seq[SocketOption] = immutable.Seq.empty,
                      halfClose: Boolean = false,
                      connectTimeout: Duration = Duration.Inf,
                      idleTimeout: Duration = Duration.Inf,
                      backoffConfig: BackoffConfig = BackoffConfig(),
                      requestBufferSize: Int = 1024,
                      overflowStrategy: OverflowStrategy = OverflowStrategy.backpressure)