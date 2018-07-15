package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import cats.data.ReaderT
import cats.implicits._
import cats.{ Monad, MonadError }
import com.github.j5ik2o.reactive.redis.RedisConnectionPool.RP
import monix.eval.Task
import org.apache.commons.pool2.impl.{
  AbandonedConfig,
  DefaultPooledObject,
  GenericObjectPool,
  GenericObjectPoolConfig
}
import org.apache.commons.pool2.{ BasePooledObjectFactory, ObjectPool, PooledObject }

import scala.concurrent.duration.Duration

private class RedisConnectionPoolFactory(connectionConfig: ConnectionConfig, disconnectTimeout: Duration = Duration.Inf)(
    implicit system: ActorSystem
) extends BasePooledObjectFactory[RedisConnection] {

  override def create(): RedisConnection = new RedisConnection(connectionConfig)

  override def destroyObject(p: PooledObject[RedisConnection]): Unit = {
    p.getObject.shutdown()
  }

  override def wrap(t: RedisConnection): PooledObject[RedisConnection] = new DefaultPooledObject(t)

}

object RedisConnectionPool {
  type RP[M[_], A] = ReaderT[M, RedisConnection, A]
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

  def apply[M[_]](connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig)(
      implicit system: ActorSystem,
      ME: MonadError[M, Throwable]
  ): RedisConnectionPool[M] = new RedisConnectionPool[M](connectionPoolConfig, connectionConfig)

}

class RedisConnectionPool[M[_]](connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig)(
    implicit system: ActorSystem,
    ME: MonadError[M, Throwable]
) {

  private val abandonedConfig: AbandonedConfig = new AbandonedConfig()
  abandonedConfig.setLogAbandoned(true)
  abandonedConfig.setRemoveAbandonedOnBorrow(true)
  abandonedConfig.setRemoveAbandonedOnMaintenance(true)
  abandonedConfig.setUseUsageTracking(true)
  abandonedConfig.setRequireFullStackTrace(true)

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
  connectionPoolConfig.minEvictableIdleTimeMillis.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setMinEvictableIdleTimeMillis(v.toMillis)
    else
      underlyingPoolConfig.setMinEvictableIdleTimeMillis(-1L)
  }
  connectionPoolConfig.evictorShutdownTimeoutMillis.foreach { v =>
    if (v.isFinite())
      underlyingPoolConfig.setEvictorShutdownTimeoutMillis(v.toMillis)
    else
      underlyingPoolConfig.setEvictorShutdownTimeoutMillis(-1L)
  }
  connectionPoolConfig.softMinEvictableIdleTimeMillis.foreach { v =>
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
  connectionPoolConfig.timeBetweenEvictionRunsMillis.foreach { v =>
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

  private val underlyingConnectionPool: ObjectPool[RedisConnection] =
    new GenericObjectPool[RedisConnection](new RedisConnectionPoolFactory(connectionConfig),
                                           underlyingPoolConfig,
                                           abandonedConfig)

  def withConnectionF[T](f: RedisConnection => M[T]): M[T] = withConnectionM(ReaderT(f))

  def withConnectionM[T](reader: RP[M, T]): M[T] = {
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

  def borrowConnection: M[RedisConnection] =
    ME.pure(underlyingConnectionPool.borrowObject())

  def returnConnection(redisClient: RedisConnection): M[Unit] =
    ME.pure(underlyingConnectionPool.returnObject(redisClient))

  def invalidateConnection(redisClient: RedisConnection): M[Unit] =
    ME.pure(underlyingConnectionPool.invalidateObject(redisClient))

  def numActive: Int = underlyingConnectionPool.getNumActive

  def numIdle: Int = underlyingConnectionPool.getNumIdle

  def clear(): Unit = underlyingConnectionPool.clear()

  def dispose(): Unit = underlyingConnectionPool.close()

}
