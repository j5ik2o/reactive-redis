package com.github.j5ik2o.reactive.redis

import org.scalatest.{ BeforeAndAfterAll, Suite }
import redis.embedded.RedisExecProvider
import redis.embedded.util.{ Architecture, OS }

trait RedisSpecSupport extends RandomPortSupport with Suite with BeforeAndAfterAll {
  private var _redisServer: RedisServer = _

  def redisServer: RedisServer = _redisServer

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    _redisServer = new RedisServer(
      RedisExecProvider.defaultProvider().`override`(OS.MAC_OS_X, Architecture.x86_64, "redis-server-4.0.app"),
      temporaryServerPort()
    )
    redisServer.start()
  }

  override protected def afterAll(): Unit = {
    _redisServer.stop()
    super.afterAll()
  }
}
