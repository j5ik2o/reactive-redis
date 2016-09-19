package com.github.j5ik2o.reactive.redis.connection

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Source, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.connection.ConnectionProtocol._
import com.github.j5ik2o.reactive.redis.{ ActorSpec, ServerBootable, StreamAPI }

import scala.concurrent.Future

class ConnectionStreamAPISpec
  extends ActorSpec(ActorSystem("ConnectionStreamAPISpec"))
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
    assert(api.run(api.quit).futureValue == Seq(QuitSucceeded))
    system.terminate()
    super.afterAll()
  }

  describe("ConnectionStreamAPI") {
    it("select") {
      //Source.single(1).concat(Source.single(2)).fold(0){(acc, in) => acc + in}.via(Flow[Int].map{e => e * 2}).runForeach(println)
      assert(api.run(api.select(1).concat(api.select(2))).futureValue == Seq(SelectSucceeded, SelectSucceeded))

    }
  }

}
