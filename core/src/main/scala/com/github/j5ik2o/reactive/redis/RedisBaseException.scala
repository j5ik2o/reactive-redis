package com.github.j5ik2o.reactive.redis

abstract class RedisBaseException(message: Option[String], cause: Option[Throwable])
    extends Exception(message.orNull, cause.orNull)
