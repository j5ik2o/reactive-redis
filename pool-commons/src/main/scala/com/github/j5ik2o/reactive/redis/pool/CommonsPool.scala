package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.event.{ LogSource, Logging }
import akka.stream.Supervision
import cats.MonadError
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import com.github.j5ik2o.reactive.redis.pool.CommonsPool.RedisConnectionPoolFactory
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.commons.pool2.impl.{ DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig }
import org.apache.commons.pool2.{ BasePooledObjectFactory, PooledObject }

import scala.concurrent.duration._

case class RedisConnectionPoolable(index: Int, redisConnection: RedisConnection) extends RedisConnection {
  override def id: UUID = redisConnection.id

  override def shutdown(): Unit = redisConnection.shutdown()

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = redisConnection.send(cmd)
}

case class CommonsPool[M[_]](connectionPoolConfig: CommonsPoolConfig,
                             peerConfigs: Seq[PeerConfig],
                             supervisionDecider: Option[Supervision.Decider] = None,
                             validationTimeout: FiniteDuration = 3 seconds)(
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

  private val underlyingPoolConfig: GenericObjectPoolConfig[RedisConnectionPoolable] =
    new GenericObjectPoolConfig[RedisConnectionPoolable]()

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

  connectionPoolConfig.maxTotal.foreach(v => underlyingPoolConfig.setMaxTotal(v / peerConfigs.size))
  connectionPoolConfig.maxIdle.foreach(v => underlyingPoolConfig.setMaxIdle(v / peerConfigs.size))
  connectionPoolConfig.minIdle.foreach(v => underlyingPoolConfig.setMinIdle(v / peerConfigs.size))

  private def underlyingConnectionPool(index: Int, peerConfig: PeerConfig): GenericObjectPool[RedisConnectionPoolable] =
    new GenericObjectPool[RedisConnectionPoolable](
      new RedisConnectionPoolFactory(index, peerConfig, supervisionDecider, validationTimeout),
      underlyingPoolConfig
    )

  private val underlyingConnectionPools: Seq[GenericObjectPool[RedisConnectionPoolable]] = {
    val results = peerConfigs.zipWithIndex.map { case (e, index) => underlyingConnectionPool(index, e) }
    if (connectionPoolConfig.abandonedConfig.nonEmpty)
      results.foreach(_.setAbandonedConfig(abandonedConfig))
    results
  }

  private val index = new AtomicLong(1L)

  private def getUnderlyingConnectionPool: GenericObjectPool[RedisConnectionPoolable] = {
    val idx = index.getAndIncrement().toInt % underlyingConnectionPools.size
    underlyingConnectionPools(idx)
  }

  override def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T] = {
    var con: RedisConnectionPoolable = null
    try {
      con = getUnderlyingConnectionPool.borrowObject()
      reader(con)
    } finally {
      if (con != null)
        underlyingConnectionPools(con.index).returnObject(con)
    }
  }

  override def borrowConnection: M[RedisConnection] =
    try {
      ME.pure(getUnderlyingConnectionPool.borrowObject())
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  override def returnConnection(connection: RedisConnection): M[Unit] =
    try {
      connection match {
        case c: RedisConnectionPoolable =>
          ME.pure(underlyingConnectionPools(c.index).returnObject(c))
        case _ =>
          throw new IllegalArgumentException("Invalid connection class")
      }
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  def invalidateConnection(connection: RedisConnection): M[Unit] =
    try {
      connection match {
        case c: RedisConnectionPoolable =>
          ME.pure(underlyingConnectionPools(c.index).invalidateObject(c))
        case _ =>
          throw new IllegalArgumentException("Invalid connection class")
      }
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  override def numActive: Int = underlyingConnectionPools.foldLeft(0)((r, e) => r + e.getNumActive)

  def numIdle: Int = underlyingConnectionPools.foldLeft(0)((r, e) => r + e.getNumIdle)

  override def clear(): Unit = underlyingConnectionPools.foreach(_.clear())

  override def dispose(): Unit = underlyingConnectionPools.foreach(_.close())

}

object CommonsPool {
  private class RedisConnectionPoolFactory(index: Int,
                                           peerConfig: PeerConfig,
                                           supervisionDecider: Option[Supervision.Decider],
                                           validationTimeout: FiniteDuration)(
      implicit system: ActorSystem,
      scheduler: Scheduler
  ) extends BasePooledObjectFactory[RedisConnectionPoolable] {

    implicit val logSource: LogSource[RedisConnectionPoolFactory] = new LogSource[RedisConnectionPoolFactory] {
      override def genString(o: RedisConnectionPoolFactory): String  = o.getClass.getName
      override def getClazz(o: RedisConnectionPoolFactory): Class[_] = o.getClass
    }

    val log = Logging(system, this)

    private val redisClient = RedisClient()

    override def create(): RedisConnectionPoolable =
      RedisConnectionPoolable(index, RedisConnection(peerConfig, supervisionDecider))

    override def destroyObject(p: PooledObject[RedisConnectionPoolable]): Unit =
      p.getObject.shutdown()

    override def wrap(t: RedisConnectionPoolable): PooledObject[RedisConnectionPoolable] = new DefaultPooledObject(t)

    override def validateObject(p: PooledObject[RedisConnectionPoolable]): Boolean = {
      val connection = p.getObject
      redisClient.validate(validationTimeout).run(connection)
    }

  }
}