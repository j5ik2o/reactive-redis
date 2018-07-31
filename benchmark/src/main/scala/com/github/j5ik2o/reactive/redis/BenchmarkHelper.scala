package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.io.Inet.SO.{ ReceiveBufferSize, SendBufferSize }
import akka.io.Tcp.SO.{ KeepAlive, TcpNoDelay }
import com.github.j5ik2o.reactive.redis.pool._
import monix.eval.Task
import monix.execution.Scheduler
import redis.clients.jedis.JedisPool
import redis.{ RedisClientPool, RedisServer }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
trait BenchmarkHelper {
  implicit val system: ActorSystem  = ActorSystem()
  implicit val scheduler: Scheduler = Scheduler(system.dispatcher)
  val client: RedisClient           = RedisClient()
  val sizePerPeer: Int              = 1
  val WAIT_IN_SEC: Int              = 1000 * 3

  val redisTestServer: RedisTestServer = new RedisTestServer()

  private var _rediscalaPool: RedisClientPool = _

  def rediscalaPool: RedisClientPool = _rediscalaPool

  private var _pool: RedisConnectionPool[Task] = _

  def pool: RedisConnectionPool[Task] = _pool

  private var _jedisPool: JedisPool = _

  def jedisPool: JedisPool = _jedisPool

  def fixture(): Unit

  def setup(): Unit = {
    redisTestServer.start()
    Thread.sleep(WAIT_IN_SEC)
    _jedisPool = new JedisPool("127.0.0.1", redisTestServer.getPort)
    val peerConfig: PeerConfig =
      PeerConfig(
        new InetSocketAddress("127.0.0.1", redisTestServer.getPort),
        options = Vector(TcpNoDelay(true), KeepAlive(true), SendBufferSize(2048), ReceiveBufferSize(2048)),
        requestBufferSize = Int.MaxValue
      )
    // _pool = StormpotPool.ofSingle(StormpotConfig(), peerConfig, RedisConnection(_, _))
    // _pool = ScalaPool.ofSingle(ScalaPoolConfig(), peerConfig, RedisConnection(_, _))
    // _pool = FOPPool.ofSingle(FOPConfig(), peerConfig, RedisConnection(_, _))
    _pool = CommonsPool.ofSingle(CommonsPoolConfig(), peerConfig, RedisConnection(_, _))
    //_pool = RedisConnectionPool.ofSingleRoundRobin(sizePerPeer, peerConfig, RedisConnection(_, _))
    _rediscalaPool = _root_.redis.RedisClientPool(List(RedisServer("127.0.0.1", redisTestServer.getPort)))
    Thread.sleep(WAIT_IN_SEC)
    fixture()
  }

  def tearDown(): Unit = {
    _pool.dispose()
    redisTestServer.stop()
    Await.result(system.terminate(), Duration.Inf)
  }
}
