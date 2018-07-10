package com.github.j5ik2o.reactive.redis

case class HttpRequestSendException(message: String, cause: Option[Throwable] = None)
    extends RedisBaseException(Some(message), cause)
