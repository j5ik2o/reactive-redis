package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import monix.eval.Task
import monix.execution.Scheduler
import org.openjdk.jmh.annotations.{ Level, Setup, TearDown }
import redis.{ RedisClientPool, RedisServer }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
trait BenchmarkHelper {
  implicit val system: ActorSystem  = ActorSystem()
  implicit val scheduler: Scheduler = Scheduler(system.dispatcher)
  val client: RedisClient           = RedisClient()
  val sizePerPeer: Int              = 1

  val redisTestServer: RedisTestServer = new RedisTestServer()

  val rediscalaClient: redis.RedisClient = _root_.redis.RedisClient()

  var _rediscalaPool: RedisClientPool = _

  def rediscalaPool: RedisClientPool = _rediscalaPool

  private var _pool: RedisConnectionPool[Task] = _

  def pool: RedisConnectionPool[Task] = _pool

  def fixture(): Unit

  @Setup(Level.Trial)
  def setup: Unit = {
    redisTestServer.start()
    Thread.sleep(1000)
    val peerConfig: PeerConfig =
      PeerConfig(new InetSocketAddress("127.0.0.1", redisTestServer.getPort), requestBufferSize = Int.MaxValue)
    _pool = RedisConnectionPool.ofSingleRoundRobin(sizePerPeer, peerConfig, RedisConnection(_, _))
    _rediscalaPool = _root_.redis.RedisClientPool(List(RedisServer("127.0.0.1", redisTestServer.getPort)))
    fixture()
  }

  @TearDown(Level.Trial)
  def tearDown: Unit = {
    _pool.dispose()
    redisTestServer.stop()
    Await.result(system.terminate(), Duration.Inf)
  }
}
