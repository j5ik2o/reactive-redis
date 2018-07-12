package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import cats.{ Monad, MonadError }
import cats.implicits._
import monix.eval.Task
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig }
import org.apache.commons.pool2.{ BasePooledObjectFactory, ObjectPool, PooledObject }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class ConnectionPoolConfig(maxActive: Int = 8,
                                blockWhenExhausted: Boolean = true,
                                maxWait: Long = -1L,
                                maxIdle: Int = 8,
                                minIdle: Int = 0,
                                testOnBorrow: Boolean = false,
                                testOnReturn: Boolean = false,
                                timeBetweenEvictionRunsMillis: Long = -1L,
                                numTestsPerEvictionRun: Int = 3,
                                minEvictableIdleTimeMillis: Long = 1800000L,
                                testWhileIdle: Boolean = false,
                                softMinEvictableIdleTimeMillis: Long = 1800000L,
                                lifo: Boolean = true)

private class RedisConnectionPoolFactory(connectionConfig: ConnectionConfig, disconnectTimeout: Duration = Duration.Inf)(implicit system: ActorSystem)
    extends BasePooledObjectFactory[RedisConnection] {

  override def create(): RedisConnection = new RedisConnection(connectionConfig)

  override def destroyObject(p: PooledObject[RedisConnection]): Unit = {
    import monix.execution.Scheduler.Implicits.global
    Await.result(p.getObject.shutdown().runAsync, disconnectTimeout)
  }

  override def wrap(t: RedisConnection): PooledObject[RedisConnection] = new DefaultPooledObject(t)

}

object RedisConnectionPool {

  implicit val taskMonadError = new MonadError[Task, Throwable] {
    private val taskMonad = implicitly[Monad[Task]]

    override def pure[A](x: A): Task[A] = taskMonad.pure(x)

    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = taskMonad.flatMap(fa)(f)

    override def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = taskMonad.tailRecM(a)(f)

    override def raiseError[A](e: Throwable): Task[A] = Task.raiseError(e)

    override def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] = fa.onErrorRecoverWith {
      case t: Throwable => f(t)
    }
  }
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
      client <- borrowClient
      result <- f(client)
      _      <- returnClient(client)
    } yield result
  }

  def borrowClient: M[RedisConnection] =
    ME.pure(connectionPool.borrowObject())

  def returnClient(redisClient: RedisConnection): M[Unit] =
    ME.pure(connectionPool.returnObject(redisClient))

  def invalidateClient(redisClient: RedisConnection): M[Unit] =
    ME.pure(connectionPool.invalidateObject(redisClient))

  def numActive: Int = connectionPool.getNumActive

  def numIdle: Int = connectionPool.getNumIdle

  def clear(): Unit = connectionPool.clear()

  def dispose(): Unit = connectionPool.close()

}
