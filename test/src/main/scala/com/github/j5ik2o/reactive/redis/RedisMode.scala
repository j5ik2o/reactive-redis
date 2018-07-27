package com.github.j5ik2o.reactive.redis

sealed trait RedisMode

object RedisMode {

  case object Standalone extends RedisMode

  case object Sentinel extends RedisMode

  case object Cluster extends RedisMode

}
