package com.github.j5ik2o.reactive.redis.pool

import java.io.PrintWriter

import org.apache.commons.pool2.impl.EvictionPolicy

import scala.concurrent.duration.Duration

/**
  *
  * @param removeAbandonedOnBorrow
  * @param removeAbandonedOnMaintenance
  * @param removeAbandonedTimeout Time until disconnect the connection.
  * @param logAbandoned
  * @param requireFullStackTrace
  * @param logWriter
  * @param useUsageTracking
  */
final case class CommonsAbandonedConfig(removeAbandonedOnBorrow: Option[Boolean] = None,
                                        removeAbandonedOnMaintenance: Option[Boolean] = None,
                                        removeAbandonedTimeout: Option[Duration] = None,
                                        logAbandoned: Option[Boolean] = None,
                                        requireFullStackTrace: Option[Boolean] = None,
                                        logWriter: Option[PrintWriter] = None,
                                        useUsageTracking: Option[Boolean] = None)

/**
  *
  * @param lifo LIFO option. default is true.
  * @param fairness default is false.
  * @param maxWaitMillis default is Inf
  * @param minEvictableIdleTime Life time of the idle connection. 1800000 millis
  * @param evictorShutdownTimeout default is 10000 millis
  * @param softMinEvictableIdleTime default is Inf
  * @param blockWhenExhausted default is true
  * @param evictionPolicy
  * @param evictionPolicyClassName
  * @param testOnCreate default is false
  * @param testOnBorrow default is false
  * @param testOnReturn default is false
  * @param testWhileIdle default is false
  * @param numTestsPerEvictionRun default is 3.
  * @param timeBetweenEvictionRuns Interval time to check the connection in the idle state. default is Inf
  * @param jmxEnabled default is true
  * @param jmxNamePrefix default is "pool"
  * @param jmxNameBase default is null
  * @param sizePerPeer number of max total connections
  * @param maxIdlePerPeer number of max idle connections
  * @param minIdlePerPeer number of min idle connections
  * @param abandonedConfig
  */
final case class CommonsPoolConfig(
    lifo: Option[Boolean] = None,
    fairness: Option[Boolean] = None,
    maxWaitMillis: Option[Duration] = None,
    minEvictableIdleTime: Option[Duration] = None,
    evictorShutdownTimeout: Option[Duration] = None,
    softMinEvictableIdleTime: Option[Duration] = None,
    blockWhenExhausted: Option[Boolean] = None,
    evictionPolicy: Option[EvictionPolicy[RedisConnectionPoolable]] = None,
    evictionPolicyClassName: Option[String] = None,
    testOnCreate: Option[Boolean] = None,
    testOnBorrow: Option[Boolean] = None,
    testOnReturn: Option[Boolean] = None,
    testWhileIdle: Option[Boolean] = None,
    numTestsPerEvictionRun: Option[Int] = None,
    timeBetweenEvictionRuns: Option[Duration] = None,
    jmxEnabled: Option[Boolean] = None,
    jmxNamePrefix: Option[String] = None,
    jmxNameBase: Option[String] = None,
    sizePerPeer: Option[Int] = None,
    maxIdlePerPeer: Option[Int] = None,
    minIdlePerPeer: Option[Int] = None,
    abandonedConfig: Option[CommonsAbandonedConfig] = None
)
