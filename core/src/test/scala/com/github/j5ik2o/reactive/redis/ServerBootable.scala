package com.github.j5ik2o.reactive.redis

import com.github.j5ik2o.reactive.redis.server.ServerProtocol.FlushDBRequest
import com.github.j5ik2o.reactive.redis.server.ServerCommandRequests
import org.scalatest.BeforeAndAfter

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.pattern.ask

trait ServerBootable extends BeforeAndAfter {
  this: ActorSpec =>

  import RedisCommandRequests._

  val testServer: TestServer = new TestServer()

  import system.dispatcher

  override protected def beforeAll(): Unit = {
    testServer.start()
  }

  override protected def afterAll(): Unit = {
    testServer.stop()
  }

  after {
    executor.foreach(_.execute(flushDB).futureValue)
    if (Option(client).isDefined) {
      Await.result(client ? FlushDBRequest, Duration.Inf)
    }
  }

}
