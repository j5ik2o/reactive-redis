package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.stream.Supervision
import cats.data.NonEmptyList
import cn.danielw.fop.{ ObjectFactory, ObjectPool, PoolConfig, Poolable }
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.duration._

final case class FOPConnectionWithIndex(index: Int, redisConnection: RedisConnection) extends RedisConnection {
  override def id: UUID                                                  = redisConnection.id
  override def peerConfig: PeerConfig                                    = redisConnection.peerConfig
  override def shutdown(): Unit                                          = redisConnection.shutdown()
  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = redisConnection.send(cmd)

}

object FOPPool {

  def ofSingle(connectionPoolConfig: FOPConfig,
               peerConfig: PeerConfig,
               newConnection: (PeerConfig, Option[Supervision.Decider], RedisConnectionMode) => RedisConnection,
               redisConnectionMode: RedisConnectionMode = RedisConnectionMode.QueueMode,
               supervisionDecider: Option[Supervision.Decider] = None)(
      implicit system: ActorSystem,
      scheduler: Scheduler
  ): FOPPool =
    new FOPPool(connectionPoolConfig,
                NonEmptyList.of(peerConfig),
                newConnection,
                redisConnectionMode,
                supervisionDecider)

  def ofMultiple(connectionPoolConfig: FOPConfig,
                 peerConfigs: NonEmptyList[PeerConfig],
                 newConnection: (PeerConfig, Option[Supervision.Decider], RedisConnectionMode) => RedisConnection,
                 redisConnectionMode: RedisConnectionMode = RedisConnectionMode.QueueMode,
                 supervisionDecider: Option[Supervision.Decider] = None)(
      implicit system: ActorSystem,
      scheduler: Scheduler
  ): FOPPool = new FOPPool(connectionPoolConfig, peerConfigs, newConnection, redisConnectionMode, supervisionDecider)

  private def createFactory(
      index: Int,
      connectionPoolConfig: FOPConfig,
      peerConfig: PeerConfig,
      newConnection: (PeerConfig, Option[Supervision.Decider], RedisConnectionMode) => RedisConnection,
      redisConnectionMode: RedisConnectionMode = RedisConnectionMode.QueueMode,
      supervisionDecider: Option[Supervision.Decider]
  )(implicit system: ActorSystem, scheduler: Scheduler): ObjectFactory[RedisConnection] =
    new ObjectFactory[RedisConnection] {
      val redisClient = RedisClient()
      override def create(): RedisConnection =
        FOPConnectionWithIndex(index, newConnection(peerConfig, supervisionDecider, redisConnectionMode))

      override def destroy(t: RedisConnection): Unit = {
        t.shutdown()
      }

      override def validate(t: RedisConnection): Boolean = {
        redisClient.validate(connectionPoolConfig.validationTimeout.getOrElse(3 seconds)).run(t)
      }
    }

}

final class FOPPool private (
    val connectionPoolConfig: FOPConfig,
    val peerConfigs: NonEmptyList[PeerConfig],
    val newConnection: (PeerConfig, Option[Supervision.Decider], RedisConnectionMode) => RedisConnection,
    val redisConnectionMode: RedisConnectionMode = RedisConnectionMode.QueueMode,
    val supervisionDecider: Option[Supervision.Decider] = None
)(
    implicit system: ActorSystem,
    scheduler: Scheduler
) extends RedisConnectionPool[Task] {

  private val poolConfig = new PoolConfig()
  connectionPoolConfig.maxSizePerPeer.foreach(v => poolConfig.setMaxSize(v))
  connectionPoolConfig.minSizePerPeer.foreach(v => poolConfig.setMinSize(v))
  connectionPoolConfig.maxWaitDuration.foreach(v => poolConfig.setMaxWaitMilliseconds(v.toMillis.toInt))
  connectionPoolConfig.maxIdleDuration.foreach(v => poolConfig.setMaxIdleMilliseconds(v.toMillis.toInt))
  connectionPoolConfig.partitionSizePerPeer.foreach(v => poolConfig.setPartitionSize(v))
  connectionPoolConfig.scavengeInterval.foreach(
    v => poolConfig.setScavengeIntervalMilliseconds(v.toMillis.toInt)
  )
  connectionPoolConfig.scavengeRatio.foreach(poolConfig.setScavengeRatio)

  private val index = new AtomicLong(0L)

  private val objectPools = peerConfigs.toList.zipWithIndex.map {
    case (e, index) =>
      val factory =
        FOPPool.createFactory(index, connectionPoolConfig, e, newConnection, redisConnectionMode, supervisionDecider)
      new ObjectPool(poolConfig, factory)
  }

  private def getObjectPool = objectPools(index.getAndIncrement().toInt % objectPools.size)

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var", "org.wartremover.warts.Equals"))
  override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = {
    // scalastyle:off
    var p: Poolable[RedisConnection] = null
    try {
      p = getObjectPool.borrowObject()
      reader(FOPConnection(p))
    } finally {
      if (p != null)
        p.returnObject()
    }
    // scalastyle:on
  }

  override def borrowConnection: Task[RedisConnection] = {
    try {
      val obj = getObjectPool.borrowObject()
      Task.pure(FOPConnection(obj))
    } catch {
      case t: Throwable =>
        Task.raiseError(t)
    }
  }

  override def returnConnection(redisConnection: RedisConnection): Task[Unit] = {
    redisConnection match {
      case con: FOPConnection =>
        try {
          Task.pure(con.underlying.returnObject())
        } catch {
          case t: Throwable =>
            Task.raiseError(t)
        }
      case _ =>
        throw new IllegalArgumentException("Invalid connection class")
    }
  }

  override def numActive: Int = objectPools.foldLeft(0)(_ + _.getSize)

  def numIdle: Int = objectPools.foldLeft(0)(_ + _.getSize)

  override def clear(): Unit = {}

  override def dispose(): Unit = objectPools.foreach(_.shutdown())

}
