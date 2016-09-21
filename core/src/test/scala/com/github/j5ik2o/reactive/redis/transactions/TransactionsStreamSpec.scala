package com.github.j5ik2o.reactive.redis.transactions

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisAPIExecutor, ServerBootable }

class TransactionsStreamSpec
    extends ActorSpec(ActorSystem("TransactionsStreamSpec"))
    with ServerBootable {

  import com.github.j5ik2o.reactive.redis.RedisCommandRequests._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    executor = Some(RedisAPIExecutor(address))
  }

  override protected def afterAll(): Unit = {
    executor.foreach(_.execute(quitRequest).futureValue)
    system.terminate()
    super.afterAll()
  }

  describe("TransactionsStreamAPI") {
    // --- DISCARD
    it("DISCARD") {

    }
    // --- EXEC
    it("EXEC") {

    }
    // --- MULTI
    describe("MULTI") {
      it("should be able to exec multi") {
        // val requests = multiRequest ++ setRequest("1", "a") ++ getRequest("1") ++ execRequest
        // executor.map(_.execute(requests).futureValue).foreach(println)
      }
    }
    // --- UNWATCH
    it("UNWATCH") {

    }
    // --- WATCH
    it("WATCH") {

    }

  }
}
