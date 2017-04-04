package com.github.j5ik2o.reactive.redis

import org.scalatest.BeforeAndAfter

import scala.concurrent.Await
import scala.concurrent.duration.Duration

trait ServerBootable extends BeforeAndAfter { this: ActorSpec =>

  val testServer: TestServer = new TestServer()

  override protected def beforeAll(): Unit = {
    testServer.start()
  }

  override protected def afterAll(): Unit = {
    testServer.stop()
  }

  after {}

}
