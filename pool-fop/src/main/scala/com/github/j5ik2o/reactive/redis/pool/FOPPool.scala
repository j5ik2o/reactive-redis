package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import cats.MonadError
import cn.danielw.fop.{ ObjectFactory, ObjectPool, Poolable }
import com.github.j5ik2o.reactive.redis._
import monix.execution.Scheduler

import scala.concurrent.duration._
import cn.danielw.fop.PoolConfig
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import monix.eval.Task

case class FOPConnectionWithIndex(index: Int, redisConnection: RedisConnection) extends RedisConnection {
  def id: UUID                                                  = redisConnection.id
  def shutdown(): Unit                                          = redisConnection.shutdown()
  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = redisConnection.send(cmd)
}

object FOPPool {

  private def createFactory(
      index: Int,
      connectionPoolConfig: FOPConfig,
      peerConfig: PeerConfig
  )(implicit system: ActorSystem, scheduler: Scheduler): ObjectFactory[RedisConnection] =
    new ObjectFactory[RedisConnection] {
      val redisClient                        = RedisClient()
      override def create(): RedisConnection = FOPConnectionWithIndex(index, RedisConnection(peerConfig))

      override def destroy(t: RedisConnection): Unit = {
        t.shutdown()
      }

      override def validate(t: RedisConnection): Boolean = {
        redisClient.validate(connectionPoolConfig.validationTimeout.getOrElse(3 seconds)).run(t)
      }
    }

}

case class FOPPool[M[_]](connectionPoolConfig: FOPConfig, connectionConfigs: Seq[PeerConfig])(
    implicit system: ActorSystem,
    scheduler: Scheduler,
    ME: MonadError[M, Throwable]
) extends RedisConnectionPool[M] {

  private val poolConfig = new PoolConfig()
  connectionPoolConfig.maxSize.foreach(v => poolConfig.setMaxSize(v / connectionConfigs.size))
  connectionPoolConfig.minSize.foreach(v => poolConfig.setMinSize(v / connectionConfigs.size))
  connectionPoolConfig.maxWaitDuration.foreach(v => poolConfig.setMaxWaitMilliseconds(v.toMillis.toInt))
  connectionPoolConfig.maxIdleDuration.foreach(v => poolConfig.setMaxIdleMilliseconds(v.toMillis.toInt))
  connectionPoolConfig.partitionSize.foreach(v => poolConfig.setPartitionSize(_))
  connectionPoolConfig.scavengeIntervalMilliseconds.foreach(
    v => poolConfig.setScavengeIntervalMilliseconds(v.toMillis.toInt)
  )
  connectionPoolConfig.scavengeRatio.foreach(poolConfig.setScavengeRatio)

  private val index = new AtomicLong(0L)

  private val objectPools = connectionConfigs.zipWithIndex.map {
    case (e, index) =>
      val factory = FOPPool.createFactory(index, connectionPoolConfig, e)
      new ObjectPool(poolConfig, factory)
  }

  private def getObjectPool = objectPools(index.getAndIncrement().toInt % objectPools.size)

  override def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T] = {
    var p: Poolable[RedisConnection] = null
    try {
      p = getObjectPool.borrowObject()
      reader(FOPConnection(p))
    } finally {
      if (p != null)
        p.returnObject()
    }
  }

  override def borrowConnection: M[RedisConnection] = {
    try {
      val obj = getObjectPool.borrowObject()
      ME.pure(FOPConnection(obj))
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }
  }

  override def returnConnection(redisConnection: RedisConnection): M[Unit] = {
    redisConnection match {
      case con: FOPConnection =>
        try {
          ME.pure(con.underlying.returnObject())
        } catch {
          case t: Throwable =>
            ME.raiseError(t)
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
