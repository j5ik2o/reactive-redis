package com.github.j5ik2o.reactive.redis.pool

import akka.actor.ActorSystem
import akka.stream.Supervision
import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnection }
import stormpot.{ Allocator, Slot }

final case class RedisConnectionAllocator(peerConfig: PeerConfig,
                                          newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
                                          supervisionDecider: Option[Supervision.Decider])(implicit system: ActorSystem)
    extends Allocator[RedisConnectionPoolable] {

  override def allocate(slot: Slot): RedisConnectionPoolable = {
    RedisConnectionPoolable(slot, newConnection(peerConfig, supervisionDecider))
  }

  override def deallocate(t: RedisConnectionPoolable): Unit = {
    t.close()
  }
}
