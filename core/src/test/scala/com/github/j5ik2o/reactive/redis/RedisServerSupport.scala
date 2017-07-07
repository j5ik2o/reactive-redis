package com.github.j5ik2o.reactive.redis

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait RedisServerSupport extends BeforeAndAfterAll with StrictLogging { this: ActorSpec =>

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

  def restartTestServer() = {
    _testServer.stop()
    Future {
      Thread.sleep(500)
      _testServer = new TestServer(portOpt = Some(_testServer.getPort))
      _testServer.start()
    }
  }

}
