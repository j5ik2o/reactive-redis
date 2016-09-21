package com.github.j5ik2o.reactive.redis.connection

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.connection.ConnectionProtocol._
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisAPIExecutor, ServerBootable }

class ConnectionStreamSpec
    extends ActorSpec(ActorSystem("ConnectionStreamSpec"))
    with ServerBootable {

  import com.github.j5ik2o.reactive.redis.RedisCommandRequests._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    executor = Some(RedisAPIExecutor(address))
  }

  override protected def afterAll(): Unit = {
    assert(executor.map(_.execute(quitRequest).futureValue).contains(Seq(QuitSucceeded)))
    system.terminate()
    super.afterAll()
  }

  describe("ConnectionStreamAPI") {
    it("select") {
      assert(executor.map(_.execute(selectRequest(1) ++ selectRequest(2)).futureValue).contains(Seq(SelectSucceeded, SelectSucceeded)))

    }
  }

}
