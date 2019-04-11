package com.github.j5ik2o.reactive.redis.pool

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.RedisClient
import monix.execution.Scheduler
import stormpot.{ Expiration, SlotInfo }

import scala.concurrent.duration.Duration

final case class RedisConnectionExpiration(validationTimeout: Duration)(
    implicit system: ActorSystem,
    scheduler: Scheduler
) extends Expiration[RedisConnectionPoolable] {
  private val redisClient = RedisClient()
  override def hasExpired(slotInfo: SlotInfo[_ <: RedisConnectionPoolable]): Boolean = {
    !redisClient.validate(validationTimeout).run(slotInfo.getPoolable.redisConnection)
  }
}
