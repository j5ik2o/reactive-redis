package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import monix.eval.Task

class SingleConnectionPool(redisConnection: RedisConnection)(implicit system: ActorSystem)
    extends RedisConnectionPool[Task]() {

  override def peerConfigs: Seq[PeerConfig] = Seq(redisConnection.peerConfig)

  override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = reader(redisConnection)

  override def borrowConnection: Task[RedisConnection] = Task.pure(redisConnection)

  override def returnConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

  def invalidateConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

  override def numActive: Int = 1

  override def clear(): Unit = {}

  override def dispose(): Unit = redisConnection.shutdown()

}

object SingleConnectionPool {

  def apply(peerConfig: PeerConfig)(implicit system: ActorSystem): SingleConnectionPool =
    new SingleConnectionPool(RedisConnection(peerConfig))

  def apply(redisConnection: RedisConnection)(implicit system: ActorSystem): SingleConnectionPool =
    new SingleConnectionPool(redisConnection)

}
