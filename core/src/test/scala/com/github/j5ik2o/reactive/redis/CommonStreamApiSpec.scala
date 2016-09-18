package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike }

import scala.concurrent.Future

class CommonStreamApiSpec
  extends TestKit(ActorSystem("CommonStreamApiSpec"))
    with FunSpecLike
    with BeforeAndAfterAll with ScalaFutures {

  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val testServer = new TestServer()

  var api: StringStreamApi = _


  override protected def beforeAll(): Unit = {
    testServer.start()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    api = new StringStreamApi {
      override protected val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
        Tcp().outgoingConnection(address)
    }
  }

  override protected def afterAll(): Unit = {
    api.quit.futureValue
    system.terminate()
    testServer.stop()
  }

  describe("CommonStreamApi") {
    it("CommonStreamApiSpec") {
      api.set("1", "a").futureValue
      assert(api.exists("1").futureValue)
    }
  }

}
