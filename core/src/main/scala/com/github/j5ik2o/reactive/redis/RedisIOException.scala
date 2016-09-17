package com.github.j5ik2o.reactive.redis

import java.io.IOException

case class RedisIOException(message: Option[String]) extends IOException(message.orNull)
