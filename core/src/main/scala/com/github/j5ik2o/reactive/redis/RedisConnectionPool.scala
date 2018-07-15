package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Supervision
import cats.data.ReaderT
import cats.implicits._
import cats.{ Monad, MonadError }
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig }
import org.apache.commons.pool2.{ BasePooledObjectFactory, PooledObject }

import scala.concurrent.Await
import scala.concurrent.duration._

private class RedisConnectionPoolFactory(connectionConfig: ConnectionConfig,
                                         supervisionDecider: Option[Supervision.Decider],
                                         validationTimeout: FiniteDuration)(
    implicit system: ActorSystem,
    scheduler: Scheduler
) extends BasePooledObjectFactory[RedisConnection] {

  private val redisClient = RedisClient()

  override def create(): RedisConnection = RedisConnection(connectionConfig, supervisionDecider)

  override def destroyObject(p: PooledObject[RedisConnection]): Unit =
    p.getObject.shutdown()

  override def wrap(t: RedisConnection): PooledObject[RedisConnection] = new DefaultPooledObject(t)

  override def validateObject(p: PooledObject[RedisConnection]): Boolean = {
    val connection = p.getObject
    val id         = UUID.randomUUID().toString
    Await.result(redisClient.ping(Some(id)).run(connection).runAsync, validationTimeout) == id
  }

}

object RedisConnectionPool {

  implicit val taskMonadError: MonadError[Task, Throwable] = new MonadError[Task, Throwable] {
    private val taskMonad = implicitly[Monad[Task]]

    override def pure[A](x: A): Task[A] = taskMonad.pure(x)

    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = taskMonad.flatMap(fa)(f)

    override def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = taskMonad.tailRecM(a)(f)

    override def raiseError[A](e: Throwable): Task[A] = Task.raiseError(e)

    override def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] = fa.onErrorRecoverWith {
      case t: Throwable => f(t)
    }
  }

  def ofCommons[M[_]](connectionPoolConfig: ConnectionPoolConfig,
                      connectionConfig: ConnectionConfig,
                      supervisionDecider: Option[Supervision.Decider] = None,
                      validationTimeout: FiniteDuration = 3 seconds)(
      implicit system: ActorSystem,
      scheduler: Scheduler,
      ME: MonadError[M, Throwable]
  ): RedisConnectionPool[M] =
    new RedisConnectionPoolCommons[M](connectionPoolConfig, connectionConfig, supervisionDecider, validationTimeout)

}

abstract class RedisConnectionPool[M[_]](implicit ME: MonadError[M, Throwable]) {

  def withConnectionF[T](f: RedisConnection => M[T]): M[T] = withConnectionM(ReaderT(f))

  def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T] = {
    for {
      connection <- borrowConnection
      result <- reader
        .run(connection)
        .flatMap { result =>
          returnConnection(connection).map(_ => result)
        }
        .recoverWith {
          case error =>
            invalidateConnection(connection).flatMap(_ => ME.raiseError(error))
        }
    } yield result
  }

  def borrowConnection: M[RedisConnection]

  def returnConnection(redisClient: RedisConnection): M[Unit]

  def invalidateConnection(redisClient: RedisConnection): M[Unit]

  def numActive: Int

  def numIdle: Int

  def clear(): Unit

  def dispose(): Unit
}

class RedisConnectionPoolCommons[M[_]](connectionPoolConfig: ConnectionPoolConfig,
                                       connectionConfig: ConnectionConfig,
                                       supervisionDecider: Option[Supervision.Decider],
                                       validationTimeout: FiniteDuration)(
    implicit system: ActorSystem,
    scheduler: Scheduler,
    ME: MonadError[M, Throwable]
) extends RedisConnectionPool[M] {

  private val abandonedConfig: org.apache.commons.pool2.impl.AbandonedConfig =
    new org.apache.commons.pool2.impl.AbandonedConfig()

  connectionPoolConfig.abandonedConfig.foreach { v =>
    v.logAbandoned.foreach(abandonedConfig.setLogAbandoned)
    v.removeAbandonedOnBorrow.foreach(abandonedConfig.setRemoveAbandonedOnBorrow)
    v.removeAbandonedOnMaintenance.foreach(abandonedConfig.setRemoveAbandonedOnMaintenance)
    v.logWriter.foreach(abandonedConfig.setLogWriter)
    v.removeAbandonedTimeout.foreach(v => abandonedConfig.setRemoveAbandonedTimeout(v.toSeconds.toInt))
    v.requireFullStackTrace.foreach(abandonedConfig.setRequireFullStackTrace)
    v.useUsageTracking.foreach(abandonedConfig.setUseUsageTracking)
  }

  private val underlyingPoolConfig: GenericObjectPoolConfig[RedisConnection] =
    new GenericObjectPoolConfig[RedisConnection]()

  connectionPoolConfig.lifo.foreach(underlyingPoolConfig.setLifo)
  connectionPoolConfig.fairness.foreach(underlyingPoolConfig.setFairness)
  connectionPoolConfig.maxWaitMillis.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setMaxWaitMillis(v.toMillis)
    else
      underlyingPoolConfig.setMaxWaitMillis(-1L)
  }
  connectionPoolConfig.minEvictableIdleTime.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setMinEvictableIdleTimeMillis(v.toMillis)
    else
      underlyingPoolConfig.setMinEvictableIdleTimeMillis(-1L)
  }
  connectionPoolConfig.evictorShutdownTimeout.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setEvictorShutdownTimeoutMillis(v.toMillis)
    else
      underlyingPoolConfig.setEvictorShutdownTimeoutMillis(-1L)
  }
  connectionPoolConfig.softMinEvictableIdleTime.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setSoftMinEvictableIdleTimeMillis(v.toMillis)
    else
      underlyingPoolConfig.setSoftMinEvictableIdleTimeMillis(-1L)
  }

  connectionPoolConfig.numTestsPerEvictionRun.foreach(underlyingPoolConfig.setNumTestsPerEvictionRun)
  connectionPoolConfig.evictionPolicy.foreach(underlyingPoolConfig.setEvictionPolicy)
  connectionPoolConfig.evictionPolicyClassName.foreach(underlyingPoolConfig.setEvictionPolicyClassName)

  connectionPoolConfig.testOnCreate.foreach(underlyingPoolConfig.setTestOnCreate)
  connectionPoolConfig.testOnBorrow.foreach(underlyingPoolConfig.setTestOnBorrow)
  connectionPoolConfig.testOnReturn.foreach(underlyingPoolConfig.setTestOnReturn)
  connectionPoolConfig.testWhileIdle.foreach(underlyingPoolConfig.setTestWhileIdle)
  connectionPoolConfig.timeBetweenEvictionRuns.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setTimeBetweenEvictionRunsMillis(v.toMillis)
    else
      underlyingPoolConfig.setTimeBetweenEvictionRunsMillis(-1L)
  }
  connectionPoolConfig.blockWhenExhausted.foreach(underlyingPoolConfig.setBlockWhenExhausted)
  connectionPoolConfig.jmxEnabled.foreach(underlyingPoolConfig.setJmxEnabled)
  connectionPoolConfig.jmxNamePrefix.foreach(underlyingPoolConfig.setJmxNamePrefix)
  connectionPoolConfig.jmxNameBase.foreach(underlyingPoolConfig.setJmxNameBase)
  connectionPoolConfig.maxTotal.foreach(underlyingPoolConfig.setMaxTotal)
  connectionPoolConfig.maxIdle.foreach(underlyingPoolConfig.setMaxIdle)
  connectionPoolConfig.minIdle.foreach(underlyingPoolConfig.setMinIdle)

  private val underlyingConnectionPool: GenericObjectPool[RedisConnection] =
    new GenericObjectPool[RedisConnection](
      new RedisConnectionPoolFactory(connectionConfig, supervisionDecider, validationTimeout),
      underlyingPoolConfig
    )
  if (connectionPoolConfig.abandonedConfig.nonEmpty)
    underlyingConnectionPool.setAbandonedConfig(abandonedConfig)

  override def borrowConnection: M[RedisConnection] =
    try {
      ME.pure(underlyingConnectionPool.borrowObject())
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  override def returnConnection(redisClient: RedisConnection): M[Unit] =
    try {
      ME.pure(underlyingConnectionPool.returnObject(redisClient))
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  override def invalidateConnection(redisClient: RedisConnection): M[Unit] =
    try {
      ME.pure(underlyingConnectionPool.invalidateObject(redisClient))
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  override def numActive: Int = underlyingConnectionPool.getNumActive

  override def numIdle: Int = underlyingConnectionPool.getNumIdle

  override def clear(): Unit = underlyingConnectionPool.clear()

  override def dispose(): Unit = underlyingConnectionPool.close()

}
