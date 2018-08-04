package com.github.j5ik2o.reactive.redis

final case class RedisRequestException(message: String, cause: Option[Throwable] = None)
    extends RedisBaseException(Some(message), cause)
