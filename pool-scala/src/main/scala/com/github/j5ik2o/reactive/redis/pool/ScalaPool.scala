package com.github.j5ik2o.reactive.redis.pool

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import cats.MonadError
import com.github.j5ik2o.reactive.redis._
import io.github.andrebeat.pool._
import monix.execution.Scheduler

import scala.concurrent.duration._

case class ScalaPool[M[_]](connectionPoolConfig: ScalaPoolConfig, peerConfigs: Seq[PeerConfig])(
    implicit system: ActorSystem,
    scheduler: Scheduler,
    ME: MonadError[M, Throwable]
) extends RedisConnectionPool[M] {

  final val DEFAULT_MAX_TOTAL          = 8
  final val DEFAULT_MAX_IDLE_TIME      = 5 seconds
  final val DEFAULT_VALIDATION_TIMEOUT = 3 seconds

  private val redisClient = RedisClient()

  private def newPool(peerConfig: PeerConfig): Pool[ResettableRedisConnection] =
    Pool[ResettableRedisConnection](
      connectionPoolConfig.sizePerPeer.getOrElse(DEFAULT_MAX_TOTAL),
      factory = { () =>
        ResettableRedisConnection(() => RedisConnection(peerConfig))
      },
      referenceType = ReferenceType.Strong,
      maxIdleTime = connectionPoolConfig.maxIdleTime.getOrElse(DEFAULT_MAX_IDLE_TIME),
      reset = { _ =>
        ()
      },
      dispose = { _.shutdown() },
      healthCheck = { con =>
        redisClient.validate(connectionPoolConfig.validationTimeout.getOrElse(DEFAULT_VALIDATION_TIMEOUT)).run(con)
      }
    )

  private val pools = peerConfigs.map(config => newPool(config))

  pools.foreach(_.fill)

  private val index = new AtomicLong(0L)

  private def getPool = pools(index.getAndIncrement().toInt % pools.size)

  override def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T] = {
    getPool.acquire() { con =>
      reader.run(con)
    }
  }

  override def borrowConnection: M[RedisConnection] = {
    try {
      ME.pure(ScalaPoolConnection(getPool.acquire()))
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }
  }

  override def returnConnection(redisConnection: RedisConnection): M[Unit] = {
    redisConnection match {
      case con: ScalaPoolConnection =>
        try {
          ME.pure(con.underlying.release())
        } catch {
          case t: Throwable =>
            ME.raiseError(t)
        }
      case _ =>
        throw new IllegalArgumentException("Invalid connection class")
    }
  }

  def invalidateConnection(redisConnection: RedisConnection): M[Unit] = {
    redisConnection match {
      case con: ScalaPoolConnection =>
        try {
          ME.pure(con.underlying.invalidate())
        } catch {
          case t: Throwable =>
            ME.raiseError(t)
        }
      case _ =>
        throw new IllegalArgumentException("Invalid connection class")
    }
  }

  override def numActive: Int = pools.foldLeft(0)(_ + _.live())

  def numIdle: Int = pools.foldLeft(0)(_ + _.size)

  override def clear(): Unit = pools.foreach(_.drain())

  override def dispose(): Unit = pools.foreach(_.close())
}
