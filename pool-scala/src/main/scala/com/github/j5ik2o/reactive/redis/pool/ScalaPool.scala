package com.github.j5ik2o.reactive.redis.pool

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.stream.Supervision
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis._
import io.github.andrebeat.pool._
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.duration._

final case class ScalaPool(connectionPoolConfig: ScalaPoolConfig,
                           peerConfigs: NonEmptyList[PeerConfig],
                           newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
                           supervisionDecider: Option[Supervision.Decider] = None)(
    implicit system: ActorSystem,
    scheduler: Scheduler
) extends RedisConnectionPool[Task] {

  val DEFAULT_MAX_TOTAL: Int                     = 8
  val DEFAULT_MAX_IDLE_TIME: FiniteDuration      = 5 seconds
  val DEFAULT_VALIDATION_TIMEOUT: FiniteDuration = 3 seconds

  private val redisClient = RedisClient()

  private def newPool(peerConfig: PeerConfig): Pool[ResettableRedisConnection] =
    Pool[ResettableRedisConnection](
      connectionPoolConfig.sizePerPeer.getOrElse(DEFAULT_MAX_TOTAL),
      factory = { () =>
        ResettableRedisConnection(() => newConnection(peerConfig, supervisionDecider))
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

  private val pools = peerConfigs.toList.map(config => newPool(config))

  pools.foreach(_.fill)

  private val index = new AtomicLong(0L)

  private def getPool = pools(index.getAndIncrement().toInt % pools.size)

  override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = {
    getPool.acquire() { con =>
      reader.run(con)
    }
  }

  override def borrowConnection: Task[RedisConnection] = {
    try {
      Task.pure(ScalaPoolConnection(getPool.acquire()))
    } catch {
      case t: Throwable =>
        Task.raiseError(t)
    }
  }

  override def returnConnection(redisConnection: RedisConnection): Task[Unit] = {
    redisConnection match {
      case con: ScalaPoolConnection =>
        try {
          Task.pure(con.underlying.release())
        } catch {
          case t: Throwable =>
            Task.raiseError(t)
        }
      case _ =>
        throw new IllegalArgumentException("Invalid connection class")
    }
  }

  def invalidateConnection(redisConnection: RedisConnection): Task[Unit] = {
    redisConnection match {
      case con: ScalaPoolConnection =>
        try {
          Task.pure(con.underlying.invalidate())
        } catch {
          case t: Throwable =>
            Task.raiseError(t)
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
