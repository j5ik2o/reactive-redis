package com.github.j5ik2o.reactive.redis

import enumeratum._

import scala.collection.immutable

sealed trait RedisConnectionSourceMode extends EnumEntry

object RedisConnectionSourceMode extends Enum[RedisConnectionSourceMode] {
  override def values: immutable.IndexedSeq[RedisConnectionSourceMode] = findValues

  case object QueueMode extends RedisConnectionSourceMode

  case object ActorMode extends RedisConnectionSourceMode

}
