package com.github.j5ik2o.reactive.redis

import org.scalatest.{BeforeAndAfterAll, Suite}

trait ServerBootable extends BeforeAndAfterAll { this: Suite =>

  val testServer: TestServer = new TestServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    testServer.start()
  }

  override protected def afterAll(): Unit = {
    testServer.stop()
    super.afterAll()
  }

}
