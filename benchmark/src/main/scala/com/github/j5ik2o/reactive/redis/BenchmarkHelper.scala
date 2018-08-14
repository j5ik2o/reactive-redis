package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.io.Inet.SO.{ ReceiveBufferSize, SendBufferSize }
import akka.io.Tcp.SO.{ KeepAlive, TcpNoDelay }
import akka.stream.OverflowStrategy
import com.github.j5ik2o.reactive.redis.pool._
import monix.eval.Task
import monix.execution.Scheduler
import redis.clients.jedis.JedisPool
import redis.{ RedisClientPool, RedisServer }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@SuppressWarnings(
  Array("org.wartremover.warts.Null", "org.wartremover.warts.Var", "org.wartremover.warts.Serializable")
)
trait BenchmarkHelper {
  implicit val system: ActorSystem  = ActorSystem()
  implicit val scheduler: Scheduler = Scheduler(system.dispatcher)
  val client: RedisClient           = RedisClient()
  val sizePerPeer: Int              = 3
  val WAIT_IN_SEC: Int              = 1000 * 3

  val redisTestServer: RedisTestServer = new RedisTestServer()

  private var _rediscalaPool: RedisClientPool = _

  def rediscalaPool: RedisClientPool = _rediscalaPool

  private var _poolOfJedisQueue: RedisConnectionPool[Task] = _
  private var _poolOfJedisActor: RedisConnectionPool[Task] = _

  private var _poolOfDefaultQueue: RedisConnectionPool[Task] = _
  private var _poolOfDefaultActor: RedisConnectionPool[Task] = _

  def reactiveRedisPoolOfJedisQueue: RedisConnectionPool[Task] = _poolOfJedisQueue
  def reactiveRedisPoolOfJedisActor: RedisConnectionPool[Task] = _poolOfJedisActor

  def reactiveRedisPoolOfDefaultQueue: RedisConnectionPool[Task] = _poolOfDefaultQueue
  def reactiveRedisPoolOfDefaultActor: RedisConnectionPool[Task] = _poolOfDefaultActor

  private var _jedisPool: JedisPool = _

  def jedisPool: JedisPool = _jedisPool

  private var _scalaRedisPool: com.redis.RedisClientPool = _

  def scalaRedisPool: com.redis.RedisClientPool = _scalaRedisPool

  def fixture(): Unit

  def setup(): Unit = {
    redisTestServer.start()
    Thread.sleep(WAIT_IN_SEC)
    _jedisPool = new JedisPool("127.0.0.1", redisTestServer.getPort)
    val peerConfig: PeerConfig =
      PeerConfig(
        new InetSocketAddress("127.0.0.1", redisTestServer.getPort),
        options = Vector(TcpNoDelay(true), KeepAlive(true), SendBufferSize(2048), ReceiveBufferSize(2048)),
        overflowStrategyOnSourceQueueMode = OverflowStrategy.dropNew,
        requestBufferSize = Int.MaxValue
      )
    // _pool = StormpotPool.ofSingle(StormpotConfig(), peerConfig, RedisConnection(_, _))
    // _pool = ScalaPool.ofSingle(ScalaPoolConfig(), peerConfig, RedisConnection(_, _))
    // _pool = FOPPool.ofSingle(FOPConfig(), peerConfig, RedisConnection(_, _))
    //_pool = RedisConnectionPool.ofSingleRoundRobin(sizePerPeer, peerConfig, RedisConnection(_, _))

    _poolOfDefaultQueue =
      CommonsPool.ofSingle(CommonsPoolConfig(), peerConfig.withConnectionSourceQueueMode, RedisConnection.apply)
    _poolOfDefaultActor =
      CommonsPool.ofSingle(CommonsPoolConfig(), peerConfig.withConnectionSourceActorMode, RedisConnection.apply)

    _poolOfJedisQueue =
      CommonsPool.ofSingle(CommonsPoolConfig(), peerConfig.withConnectionSourceQueueMode, RedisConnection.ofJedis)
    _poolOfJedisActor =
      CommonsPool.ofSingle(CommonsPoolConfig(), peerConfig.withConnectionSourceActorMode, RedisConnection.ofJedis)

    _rediscalaPool = _root_.redis.RedisClientPool(List(RedisServer("127.0.0.1", redisTestServer.getPort)))
    _scalaRedisPool = new com.redis.RedisClientPool("127.0.0.1", redisTestServer.getPort)
    Thread.sleep(WAIT_IN_SEC)
    fixture()
  }

  def tearDown(): Unit = {
    _poolOfJedisQueue.dispose()
    _poolOfJedisActor.dispose()
    _poolOfDefaultQueue.dispose()
    _poolOfDefaultActor.dispose()
    redisTestServer.stop()
    Await.result(system.terminate(), Duration.Inf)
  }
}
