package com.github.j5ik2o.reactive.redis

import scala.collection.mutable.ArrayBuffer

trait RedisSpecSupport extends RandomPortSupport with Suite with BeforeAndAfterAll {

  def waitFor(): Unit

  private var _redisMasterServer: RedisTestServer = _

  private val _redisSalveServers: ArrayBuffer[RedisTestServer] = ArrayBuffer.empty

  def redisMasterServer: RedisTestServer       = _redisMasterServer
  def redisSlaveServers: List[RedisTestServer] = _redisSalveServers.toList

  def newSalveServers(masterPort: Int)(n: Int): List[RedisTestServer] =
    (for (_ <- 1 to n) yield newRedisServer(Some(masterPort))).toList

  def newRedisServer(masterPortOpt: Option[Int] = None): RedisTestServer = {
    new RedisTestServer(masterPortOpt = masterPortOpt)
  }

  def startMasterServer(): Unit = {
    _redisMasterServer = new RedisTestServer()
    _redisMasterServer.start()
  }

  def stopMasterServer(): Unit = {
    _redisMasterServer.stop()
  }

  def startSlaveServers(): Unit = {
    _redisSalveServers.clear()
    _redisSalveServers.append(newSalveServers(_redisMasterServer.getPort)(1): _*)
    _redisSalveServers.foreach { slaveServer =>
      slaveServer.start()
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
