package com.github.j5ik2o.reactive.redis

import org.apache.commons.pool2.impl.EvictionPolicy

import scala.concurrent.duration.Duration

case class ConnectionPoolConfig(
    lifo: Option[Boolean] = None,
    fairness: Option[Boolean] = None,
    maxWaitMillis: Option[Duration] = None,
    minEvictableIdleTimeMillis: Option[Duration] = None,
    evictorShutdownTimeoutMillis: Option[Duration] = None,
    softMinEvictableIdleTimeMillis: Option[Duration] = None,
    blockWhenExhausted: Option[Boolean] = None,
    evictionPolicy: Option[EvictionPolicy[RedisConnection]] = None,
    evictionPolicyClassName: Option[String] = None,
    testOnCreate: Option[Boolean] = None,
    testOnBorrow: Option[Boolean] = None,
    testOnReturn: Option[Boolean] = None,
    testWhileIdle: Option[Boolean] = None,
    numTestsPerEvictionRun: Option[Int] = None,
    timeBetweenEvictionRunsMillis: Option[Duration] = None,
    jmxEnabled: Option[Boolean] = None,
    jmxNamePrefix: Option[String] = None,
    jmxNameBase: Option[String] = None,
    maxTotal: Option[Int] = None,
    maxIdle: Option[Int] = None,
    minIdle: Option[Int] = None
)
