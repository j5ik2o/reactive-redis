package com.github.j5ik2o.reactive.redis.transactions

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Source, Tcp }
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.{ ActorSpec, ServerBootable, StreamAPI }
import org.scalatest.FunSpecLike

import scala.concurrent.Future

class TransactionsStreamAPISpec
  extends ActorSpec(ActorSystem("TransactionsStreamAPISpec"))
    with ServerBootable {

  import system.dispatcher

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    val api = new StreamAPI {
      override protected val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
        Tcp().outgoingConnection(address)
    }
    apiRef.set(api)
  }

  override protected def afterAll(): Unit = {
    api.run(api.quit).futureValue
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
        //Source.single("A").concat(Source.single("B")).runForeach(println)
        //Source.single("A").mapConcat(e => List(e, "B")).runForeach(println)

//        api.multi(
//          api.setSource("1", "a").concat(api.getSource("1"))
//        ).futureValue
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
