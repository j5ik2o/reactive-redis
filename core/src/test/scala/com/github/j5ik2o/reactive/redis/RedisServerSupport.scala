package com.github.j5ik2o.reactive.redis

import org.scalatest.{ BeforeAndAfterAll, Suite }
import scala.concurrent.ExecutionContext.Implicits.global

trait RedisServerSupport extends BeforeAndAfterAll { this: Suite =>

  private var _testServer: TestServer = _

  def testServer = _testServer

  override def beforeAll(): Unit = {
    super.beforeAll()
    _testServer = new TestServer()
    _testServer.start()
  }

  override protected def afterAll(): Unit = {
    _testServer.stop()
    super.afterAll()
  }

}
