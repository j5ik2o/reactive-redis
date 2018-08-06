package com.github.j5ik2o.reactive.redis

import enumeratum._

import scala.collection.immutable

sealed trait RedisConnectionMode extends EnumEntry

object RedisConnectionMode extends Enum[RedisConnectionMode] {
  override def values: immutable.IndexedSeq[RedisConnectionMode] = findValues

  case object QueueMode extends RedisConnectionMode

  case object ActorMode extends RedisConnectionMode

}
