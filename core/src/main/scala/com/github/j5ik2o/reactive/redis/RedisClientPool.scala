package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import org.apache.commons.pool.impl.GenericObjectPoolFactory
import org.apache.commons.pool.{ BasePoolableObjectFactory, ObjectPool }

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

case class PoolConfig(maxActive: Int = 8,
                      whenExhaustedAction: Byte = 1,
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

private class PoolFactory(clientConfig: ClientConfig)(implicit system: ActorSystem)
    extends BasePoolableObjectFactory[RedisClient] {

  override def makeObject(): RedisClient = new RedisClient(clientConfig)

  override def destroyObject(t: RedisClient): Unit = Await.result(t.shutdown(), Duration.Inf)

}

class RedisClientPool(poolConfig: PoolConfig, clientConfig: ClientConfig)(implicit system: ActorSystem) {

  import poolConfig._

  private val poolFactory = new GenericObjectPoolFactory[RedisClient](
    new PoolFactory(clientConfig),
    maxActive,
    whenExhaustedAction,
    maxWait,
    maxIdle,
    minIdle,
    testOnBorrow,
    testOnReturn,
    timeBetweenEvictionRunsMillis,
    numTestsPerEvictionRun,
    minEvictableIdleTimeMillis,
    testWhileIdle,
    softMinEvictableIdleTimeMillis,
    lifo
  )

  private val pool: ObjectPool[RedisClient] = poolFactory.createPool()

  def withClient[T](f: RedisClient => T): Try[T] = {
    for {
      client <- borrowClient
      result <- Try(f(client))
      _      <- returnClient(client)
    } yield result
  }

  def borrowClient: Try[RedisClient] = Try(pool.borrowObject())

  def returnClient(redisClient: RedisClient): Try[Unit] = Try(pool.returnObject(redisClient))

  def invalidateClient(redisClient: RedisClient): Try[Unit] = Try(pool.invalidateObject(redisClient))

  def numActive: Int = pool.getNumActive

  def numIdle: Int = pool.getNumIdle

  def clear(): Unit = pool.clear()

  def dispose(): Unit = pool.close()

}
