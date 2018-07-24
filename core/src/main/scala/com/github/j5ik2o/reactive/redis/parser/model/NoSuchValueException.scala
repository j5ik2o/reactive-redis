package com.github.j5ik2o.reactive.redis.parser.model

import com.github.j5ik2o.reactive.redis.RedisBaseException

final case class NoSuchValueException(message: Option[String], cause: Option[Throwable] = None)
    extends RedisBaseException(message, cause)
