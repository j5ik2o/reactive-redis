package com.github.j5ik2o.reactive.redis.pool

import com.github.j5ik2o.reactive.redis.RedisConnection
import stormpot.{ Poolable, Slot }

final case class RedisConnectionPoolable(slot: Slot, redisConnection: RedisConnection) extends Poolable {
  override def release(): Unit = {
    slot.release(this)
  }
  def expire(): Unit = slot.expire(this)
  def close(): Unit  = redisConnection.shutdown()
}
