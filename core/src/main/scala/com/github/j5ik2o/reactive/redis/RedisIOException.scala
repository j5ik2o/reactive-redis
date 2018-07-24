package com.github.j5ik2o.reactive.redis

final case class RedisIOException(message: Option[String], cause: Option[Throwable] = None)
    extends RedisBaseException(message, cause)
