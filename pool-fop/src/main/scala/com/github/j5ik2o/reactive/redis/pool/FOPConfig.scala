package com.github.j5ik2o.reactive.redis.pool

import scala.concurrent.duration.FiniteDuration

case class FOPConfig(maxSize: Option[Int] = None,
                     minSize: Option[Int] = None,
                     maxWaitDuration: Option[FiniteDuration] = None,
                     maxIdleDuration: Option[FiniteDuration] = None,
                     partitionSize: Option[Int] = None,
                     scavengeIntervalMilliseconds: Option[FiniteDuration] = None,
                     scavengeRatio: Option[Double] = None,
                     validationTimeout: Option[FiniteDuration] = None)
