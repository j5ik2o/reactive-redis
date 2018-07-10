package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import org.scalatest.{ BeforeAndAfterAll, Suite }
import redis.embedded
import redis.embedded.util.{ Architecture, OS }
import redis.embedded.{ RedisExecProvider, RedisServer, RedisServerEx }

import scala.collection.mutable.ArrayBuffer

trait RedisSpecSupport extends RandomPortSupport with Suite with BeforeAndAfterAll {

  private var _redisMasterServer: RedisServer = _

  private val _redisSalveServers: ArrayBuffer[RedisServer] = ArrayBuffer.empty

  def redisMasterServer: RedisServer       = _redisMasterServer
  def redisSlaveServers: List[RedisServer] = _redisSalveServers.toList

  val customRedisProvider: RedisExecProvider = RedisExecProvider
    .defaultProvider()
    .`override`(OS.MAC_OS_X, Architecture.x86_64, "redis-server-4.0.app")
    .`override`(OS.UNIX, Architecture.x86_64, "redis-server-4.0.elf")

  def newSalveServers(masterPort: Int)(n: Int): List[RedisServer] =
    (for (_ <- 1 to n) yield newRedisServer(Some(masterPort))).toList

  def newRedisServer(masterPort: Option[Int] = None): embedded.RedisServer = {
    new RedisServerEx(customRedisProvider,
                      temporaryServerPort(),
                      masterPort.map(sp => new InetSocketAddress("127.0.0.1", sp)))
  }

  def startMasterServer(): Unit = {
    _redisMasterServer = newRedisServer()
    _redisMasterServer.start()
    Thread.sleep(1 * 1000)
  }

  def stopMasterServer(): Unit = {
    _redisMasterServer.stop()
  }

  def startSlaveServers(): Unit = {
    _redisSalveServers.clear()
    _redisSalveServers.append(newSalveServers(_redisMasterServer.ports.get(0))(3): _*)
    _redisSalveServers.foreach { slaveServer =>
      slaveServer.start()
      Thread.sleep(1 * 1000)
    }
  }

  def stopSlaveServers(): Unit = {
    _redisSalveServers.foreach(_.stop())
    _redisSalveServers.clear()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startMasterServer()
  }

  override protected def afterAll(): Unit = {
    stopMasterServer()
    super.afterAll()
  }
}
