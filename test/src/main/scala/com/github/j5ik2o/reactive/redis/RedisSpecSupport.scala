package com.github.j5ik2o.reactive.redis

import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

@SuppressWarnings(
  Array("org.wartremover.warts.Null", "org.wartremover.warts.Var", "org.wartremover.warts.MutableDataStructures")
)
trait RedisSpecSupport extends Suite with BeforeAndAfterAll {

  def waitFor(): Unit = {}

  private var _redisMasterServer: RedisTestServer = _

  private val _redisSalveServers: ArrayBuffer[RedisTestServer] = ArrayBuffer.empty

  def redisMasterServer: RedisTestServer = _redisMasterServer

  def redisSlaveServers: List[RedisTestServer] = _redisSalveServers.toList

  def newSalveServers(masterPort: Int)(n: Int): List[RedisTestServer] =
    (for (_ <- 1 to n) yield newRedisServer(Some(masterPort))).toList

  def newRedisServer(masterPortOpt: Option[Int] = None): RedisTestServer = {
    new RedisTestServer(masterPortOpt = masterPortOpt)
  }

  def startMasterServer()(implicit ec: ExecutionContext): Unit = {
    _redisMasterServer = new RedisTestServer()
    _redisMasterServer.start()
    waitFor()
  }

  def stopMasterServer(): Unit = {
    _redisMasterServer.stop()
    waitFor()
  }

  def startSlaveServers(size: Int)(implicit ec: ExecutionContext): Unit = {
    _redisSalveServers.clear()
    _redisSalveServers.append(newSalveServers(_redisMasterServer.getPort)(size): _*)
    _redisSalveServers.foreach { slaveServer =>
      slaveServer.start()
      waitFor()
    }
  }

  def stopSlaveServers(): Unit = {
    _redisSalveServers.foreach { slaveServer =>
      slaveServer.stop()
      waitFor()
    }
    _redisSalveServers.clear()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    import scala.concurrent.ExecutionContext.Implicits.global
    startMasterServer()
  }

  override protected def afterAll(): Unit = {
    stopMasterServer()
    super.afterAll()
  }

}
