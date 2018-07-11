package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
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

private class PoolFactory(connectionConfig: ConnectionConfig)(implicit system: ActorSystem)
    extends BasePooledObjectFactory[RedisConnection] {

  override def create(): RedisConnection = new RedisConnection(connectionConfig)

  override def wrap(t: RedisConnection): PooledObject[RedisConnection] = new DefaultPooledObject(t)

}

class RedisConnectionPool(connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig)(
    implicit system: ActorSystem
) {

  private val config = new GenericObjectPoolConfig[RedisConnection]()

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

  private val pool: ObjectPool[RedisConnection] =
    new GenericObjectPool[RedisConnection](new PoolFactory(connectionConfig), config)

  def withClient[T](f: RedisConnection => T): Try[T] = {
    for {
      client <- borrowClient
      result <- Try(f(client))
      _      <- returnClient(client)
    } yield result
  }

  def borrowClient: Try[RedisConnection] = Try(pool.borrowObject())

  def returnClient(redisClient: RedisConnection): Try[Unit] = Try(pool.returnObject(redisClient))

  def invalidateClient(redisClient: RedisConnection): Try[Unit] = Try(pool.invalidateObject(redisClient))

  def numActive: Int = pool.getNumActive

  def numIdle: Int = pool.getNumIdle

  def clear(): Unit = pool.clear()

  def dispose(): Unit = pool.close()

}
