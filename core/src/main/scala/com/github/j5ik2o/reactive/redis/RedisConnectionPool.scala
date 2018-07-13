package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import cats.implicits._
import cats.{ Monad, MonadError }
import monix.eval.Task
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig }
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

  private val config: GenericObjectPoolConfig[RedisConnection] = new GenericObjectPoolConfig[RedisConnection]()

  config.setMaxTotal(connectionPoolConfig.maxActive)
  config.setMaxIdle(connectionPoolConfig.maxIdle)
  config.setMinIdle(connectionPoolConfig.minIdle)
  config.setTestOnBorrow(connectionPoolConfig.testOnBorrow)
  config.setTestOnReturn(connectionPoolConfig.testOnReturn)
  config.setBlockWhenExhausted(connectionPoolConfig.blockWhenExhausted)
  config.setSoftMinEvictableIdleTimeMillis(connectionPoolConfig.softMinEvictableIdleTimeMillis)
  config.setMinEvictableIdleTimeMillis(connectionPoolConfig.minEvictableIdleTimeMillis)
  config.setNumTestsPerEvictionRun(connectionPoolConfig.numTestsPerEvictionRun)
  config.setTestWhileIdle(connectionPoolConfig.testWhileIdle)
  config.setLifo(connectionPoolConfig.lifo)

  private val connectionPool: ObjectPool[RedisConnection] =
    new GenericObjectPool[RedisConnection](new RedisConnectionPoolFactory(connectionConfig), config)

  def withConnection[T](f: RedisConnection => M[T]): M[T] = {
    for {
      connection <- borrowConnection
      result <- f(connection)
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
    ME.pure(connectionPool.borrowObject())

  def returnConnection(redisClient: RedisConnection): M[Unit] =
    ME.pure(connectionPool.returnObject(redisClient))

  def invalidateConnection(redisClient: RedisConnection): M[Unit] =
    ME.pure(connectionPool.invalidateObject(redisClient))

  def numActive: Int = connectionPool.getNumActive

  def numIdle: Int = connectionPool.getNumIdle

  def clear(): Unit = connectionPool.clear()

  def dispose(): Unit = connectionPool.close()

}
