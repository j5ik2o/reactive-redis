package com.github.j5ik2o.reactive.redis.pool

import scala.concurrent.duration.Duration

case class ScalaPoolConfig(maxTotal: Option[Int] = None,
                           maxIdleTime: Option[Duration] = None,
                           validationTimeout: Option[Duration] = None)