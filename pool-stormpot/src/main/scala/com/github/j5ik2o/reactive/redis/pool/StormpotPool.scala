package com.github.j5ik2o.reactive.redis.pool

import java.time.temporal.ChronoUnit
import java.time.{ Instant, ZoneId, ZonedDateTime }
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ ThreadFactory, TimeUnit }

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import cats.MonadError
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import com.github.j5ik2o.reactive.redis.pool.PoolType.{ Blaze, Queue }
import enumeratum._
import monix.eval.Task
import monix.execution.Scheduler
import stormpot._

import scala.collection.immutable
import scala.concurrent.duration._

case class RedisConnectionPoolable(slot: Slot, redisConnection: RedisConnection) extends Poolable {
  override def release(): Unit = {
    slot.release(this)
  }
  def expire() = slot.expire(this)
  def close()  = redisConnection.shutdown()
}

case class RedisConnectionAllocator(peerConfig: PeerConfig)(implicit system: ActorSystem)
    extends Allocator[RedisConnectionPoolable] {

  override def allocate(slot: Slot): RedisConnectionPoolable = {
    RedisConnectionPoolable(slot, RedisConnection(peerConfig))
  }

  override def deallocate(t: RedisConnectionPoolable): Unit = {
    t.close()
  }
}

sealed trait PoolType extends EnumEntry
object PoolType extends Enum[PoolType] {
  override def values: immutable.IndexedSeq[PoolType] = findValues
  case object Blaze extends PoolType
  case object Queue extends PoolType
}

case class StormpotConfig(poolType: PoolType = Blaze,
                          size: Option[Int] = None,
                          claimTimeout: Option[FiniteDuration] = None,
                          backgroundExpirationEnabled: Option[Boolean] = None,
                          preciseLeakDetectionEnabled: Option[Boolean] = None,
                          validationTimeout: Option[Duration] = None)

case class StormpotConnection(redisConnectionPoolable: RedisConnectionPoolable) extends RedisConnection {
  private val underlyingCon = redisConnectionPoolable.redisConnection

  override def id: UUID = underlyingCon.id

  override def shutdown(): Unit = underlyingCon.shutdown()

  override def toFlow[C <: CommandRequestBase](parallelism: Int)(
      implicit scheduler: Scheduler
  ): Flow[C, C#Response, NotUsed] = underlyingCon.toFlow(parallelism)

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = underlyingCon.send(cmd)
}

case class RedisConnectionExpiration(validationTimeout: Duration)(implicit system: ActorSystem, scheduler: Scheduler)
    extends Expiration[RedisConnectionPoolable] {
  private val redisClient = RedisClient()
  override def hasExpired(slotInfo: SlotInfo[_ <: RedisConnectionPoolable]): Boolean = {
    !redisClient.validate(validationTimeout).run(slotInfo.getPoolable.redisConnection)
  }
}

case class StormpotPool[M[_]](connectionPoolConfig: StormpotConfig, connectionConfigs: Seq[PeerConfig])(
    implicit system: ActorSystem,
    scheduler: Scheduler,
    ME: MonadError[M, Throwable]
) extends RedisConnectionPool[M] {

  final val DEFAULT_SIZE = 8

  private def newConfig(peerConfig: PeerConfig): Config[RedisConnectionPoolable] =
    new Config[RedisConnectionPoolable]
      .setAllocator(RedisConnectionAllocator(peerConfig))
      .setExpiration(RedisConnectionExpiration(connectionPoolConfig.validationTimeout.getOrElse(3 seconds)))
      .setSize(connectionPoolConfig.size.getOrElse(DEFAULT_SIZE) / connectionConfigs.size)
      .setBackgroundExpirationEnabled(connectionPoolConfig.backgroundExpirationEnabled.getOrElse(false))
      .setPreciseLeakDetectionEnabled(connectionPoolConfig.preciseLeakDetectionEnabled.getOrElse(false))

  private def newPool(
      config: Config[RedisConnectionPoolable]
  ): LifecycledResizablePool[RedisConnectionPoolable] with ManagedPool =
    connectionPoolConfig.poolType match {
      case Blaze =>
        new BlazePool[RedisConnectionPoolable](config)
      case Queue =>
        new QueuePool[RedisConnectionPoolable](config)
    }

  private val pools = connectionConfigs.map { connectionConfig =>
    val config = newConfig(connectionConfig)
    newPool(config)
  }

  private val index = new AtomicLong(0L)

  private def getPool = pools(index.getAndIncrement().toInt % pools.size)

  private val claimTieout = connectionPoolConfig.claimTimeout
    .map(v => new stormpot.Timeout(v.length, v.unit))
    .getOrElse(new stormpot.Timeout(3, TimeUnit.SECONDS))

  override def withConnectionM[T](reader: ReaderRedisConnection[M, T]): M[T] = {
    var poolable: RedisConnectionPoolable = null
    try {
      poolable = getPool.claim(claimTieout)
      reader(poolable.redisConnection)
    } finally {
      if (poolable != null)
        poolable.release()
    }
  }

  override def borrowConnection: M[RedisConnection] = {
    try {
      val c = getPool.claim(claimTieout)
      ME.pure(StormpotConnection(c))
    } catch {
      case t: Throwable =>
        ME.raiseError(t)
    }

  }

  override def returnConnection(redisConnection: RedisConnection): M[Unit] = {
    redisConnection match {
      case con: StormpotConnection =>
        try {
          ME.pure(con.redisConnectionPoolable.release())
        } catch {
          case t: Throwable =>
            ME.raiseError(t)
        }
      case _ =>
        throw new IllegalArgumentException("Invalid connection class")
    }
  }

  override def numActive: Int = pools.foldLeft(0)(_ + _.getAllocationCount.toInt)

  override def clear(): Unit = {
    pools.foreach(_.setTargetSize(0))
    pools.foreach(_.setTargetSize(connectionPoolConfig.size.getOrElse(DEFAULT_SIZE)))
  }

  override def dispose(): Unit = pools.foreach(_.shutdown())
}
