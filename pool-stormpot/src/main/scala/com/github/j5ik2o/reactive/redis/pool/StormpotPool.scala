package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Supervision
import akka.stream.scaladsl.Flow
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

case class RedisConnectionAllocator(peerConfig: PeerConfig,
                                    newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
                                    supervisionDecider: Option[Supervision.Decider])(implicit system: ActorSystem)
    extends Allocator[RedisConnectionPoolable] {

  override def allocate(slot: Slot): RedisConnectionPoolable = {
    RedisConnectionPoolable(slot, newConnection(peerConfig, supervisionDecider))
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

case class StormpotConfig(poolType: PoolType = Queue,
                          sizePerPeer: Option[Int] = None,
                          claimTimeout: Option[FiniteDuration] = None,
                          backgroundExpirationEnabled: Option[Boolean] = None,
                          preciseLeakDetectionEnabled: Option[Boolean] = None,
                          validationTimeout: Option[Duration] = None)

case class StormpotConnection(redisConnectionPoolable: RedisConnectionPoolable) extends RedisConnection {
  private val underlyingCon = redisConnectionPoolable.redisConnection

  override def id: UUID = underlyingCon.id

  override def peerConfig: PeerConfig = underlyingCon.peerConfig

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

case class StormpotPool(connectionPoolConfig: StormpotConfig,
                        peerConfigs: Seq[PeerConfig],
                        newConnection: (PeerConfig, Option[Supervision.Decider]) => RedisConnection,
                        supervisionDecider: Option[Supervision.Decider] = None)(
    implicit system: ActorSystem,
    scheduler: Scheduler
) extends RedisConnectionPool[Task] {

  final val DEFAULT_SIZE                     = 8
  final val DEFAULT_CLAIM_TIMEOUT_IN_SECONDS = 10

  private def newConfig(peerConfig: PeerConfig): Config[RedisConnectionPoolable] =
    new Config[RedisConnectionPoolable]
      .setAllocator(RedisConnectionAllocator(peerConfig, newConnection, supervisionDecider))
      .setExpiration(RedisConnectionExpiration(connectionPoolConfig.validationTimeout.getOrElse(3 seconds)))
      .setSize(connectionPoolConfig.sizePerPeer.getOrElse(DEFAULT_SIZE))
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

  private val pools = peerConfigs.map { peerConfig =>
    val config = newConfig(peerConfig)
    newPool(config)
  }

  private val index = new AtomicLong(0L)

  private def getPool = pools(index.getAndIncrement().toInt % pools.size)

  private val claimTieout = connectionPoolConfig.claimTimeout
    .map(v => new stormpot.Timeout(v.length, v.unit))
    .getOrElse(new stormpot.Timeout(DEFAULT_CLAIM_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS))

  override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = {
    var poolable: RedisConnectionPoolable = null
    try {
      logger.debug("---- start")
      poolable = getPool.claim(claimTieout)
      logger.debug(s"poolabel = $poolable")
      reader.run(poolable.redisConnection)
    } finally {
      if (poolable != null)
        poolable.release()
      logger.debug("---- finish")
    }
  }

  override def borrowConnection: Task[RedisConnection] = {
    try {
      val c = getPool.claim(claimTieout)
      Task.pure(StormpotConnection(c))
    } catch {
      case t: Throwable =>
        Task.raiseError(t)
    }

  }

  override def returnConnection(redisConnection: RedisConnection): Task[Unit] = {
    redisConnection match {
      case con: StormpotConnection =>
        try {
          Task.pure(con.redisConnectionPoolable.release())
        } catch {
          case t: Throwable =>
            Task.raiseError(t)
        }
      case _ =>
        throw new IllegalArgumentException("Invalid connection class")
    }
  }

  override def numActive: Int = pools.foldLeft(0)(_ + _.getAllocationCount.toInt)

  override def clear(): Unit = {
    pools.foreach(_.setTargetSize(0))
    pools.foreach(_.setTargetSize(connectionPoolConfig.sizePerPeer.getOrElse(DEFAULT_SIZE)))
  }

  override def dispose(): Unit = pools.foreach(_.shutdown())
}
