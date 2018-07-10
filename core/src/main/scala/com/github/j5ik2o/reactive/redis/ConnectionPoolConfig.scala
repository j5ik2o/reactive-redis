package com.github.j5ik2o.reactive.redis

case class ConnectionPoolConfig(maxActive: Int = 8,
                                blockWhenExhausted: Boolean = true,
                                maxWait: Long = -1L,
                                maxIdle: Int = 8,
                                minIdle: Int = 0,
                                testOnBorrow: Boolean = false,
                                testOnReturn: Boolean = false,
                                timeBetweenEvictionRunsMillis: Long = -1L,
                                numTestsPerEvictionRun: Int = 3,
                                minEvictableIdleTimeMillis: Long = 1800000L,
                                testWhileIdle: Boolean = false,
                                softMinEvictableIdleTimeMillis: Long = 1800000L,
                                lifo: Boolean = true)
