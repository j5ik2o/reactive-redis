package com.github.j5ik2o.reactive.redis.server

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.{ ActorSpec, ServerBootable, StreamAPI }

import scala.concurrent.Future

class ServerStreamAPISpec
  extends ActorSpec(ActorSystem("ServerStreamAPISpec"))
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
    api.quit.futureValue
    system.terminate()
    super.afterAll()
  }

  describe("ServerStreamAPI") {
    it("should be able to flush the db") {
      api.flushDB
    }
    it("should be able to flush all dbs") {
      api.flushAll
    }
  }

}
