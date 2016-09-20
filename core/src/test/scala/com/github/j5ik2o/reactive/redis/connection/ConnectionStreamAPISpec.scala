package com.github.j5ik2o.reactive.redis.connection

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.connection.ConnectionProtocol._
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisAPIExecutor, ServerBootable }

class ConnectionStreamAPISpec
  extends ActorSpec(ActorSystem("ConnectionStreamAPISpec"))
    with ServerBootable {

  import com.github.j5ik2o.reactive.redis.RedisCommandRequests._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    executor = RedisAPIExecutor(address)
  }

  override protected def afterAll(): Unit = {
    assert(executor.execute(quitRequest).futureValue == Seq(QuitSucceeded))
    system.terminate()
    super.afterAll()
  }

  describe("ConnectionStreamAPI") {
    it("select") {
      assert(executor.execute(select(1).concat(select(2))).futureValue == Seq(SelectSucceeded, SelectSucceeded))

    }
  }

}
