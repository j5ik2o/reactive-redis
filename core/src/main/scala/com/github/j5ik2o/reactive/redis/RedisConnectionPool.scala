package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import cats.MonadError
import cats.implicits._
import monix.eval.Task
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig }
import org.apache.commons.pool2.{ BasePooledObjectFactory, ObjectPool, PooledObject }

import scala.util.Try

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

private class RedisConnectionPoolFactory(connectionConfig: ConnectionConfig)(implicit system: ActorSystem)
    extends BasePooledObjectFactory[RedisConnection] {

  override def create(): RedisConnection = new RedisConnection(connectionConfig)

  override def destroyObject(p: PooledObject[RedisConnection]): Unit = super.destroyObject(p)

  override def wrap(t: RedisConnection): PooledObject[RedisConnection] = new DefaultPooledObject(t)

}

object RedisConnectionPool {

  implicit val taskMonadError = new MonadError[Task, Throwable] {
    override def pure[A](x: A): Task[A] = Task.pure(x)

    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = ???

    override def raiseError[A](e: Throwable): Task[A] = Task.raiseError(e)

    override def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] = fa.onErrorRecoverWith {
      case t: Throwable => f(t)
    }
  }
}

class RedisConnectionPool(connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig)(
    implicit system: ActorSystem
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

  def withConnection[M[_], T](f: RedisConnection => M[T])(implicit ME: MonadError[M, Throwable]): M[T] = {
    for {
      client <- borrowClient[M]
      result <- f(client)
      _      <- returnClient[M](client)
    } yield result
  }

  def borrowClient[M[_]](implicit ME: MonadError[M, Throwable]): M[RedisConnection] =
    ME.pure(connectionPool.borrowObject())

  def returnClient[M[_]](redisClient: RedisConnection)(implicit ME: MonadError[M, Throwable]): M[Unit] =
    ME.pure(connectionPool.returnObject(redisClient))

  def invalidateClient[M[_]](redisClient: RedisConnection)(implicit ME: MonadError[M, Throwable]): M[Unit] =
    ME.pure(connectionPool.invalidateObject(redisClient))

  def numActive: Int = connectionPool.getNumActive

  def numIdle: Int = connectionPool.getNumIdle

  def clear(): Unit = connectionPool.clear()

  def dispose(): Unit = connectionPool.close()

}
