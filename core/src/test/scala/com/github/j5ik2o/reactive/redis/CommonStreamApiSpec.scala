package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, FunSpecLike }

import scala.concurrent.Future

class CommonStreamApiSpec
  extends TestKit(ActorSystem("CommonStreamApiSpec"))
    with FunSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfter
    with ScalaFutures {

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

  after {
    api.flushAll.futureValue
  }

  describe("CommonStreamApi") {
    describe("exists") {
      it("should be able to exist the registration key") {
        val id = UUID.randomUUID().toString
        api.set(id, "a").futureValue
        assert(api.exists(id).futureValue)
      }
      it("shouldn't be able to exist the un-registration key") {
        val id = UUID.randomUUID().toString
        assert(!api.exists(id).futureValue)
      }
    }
    describe("del") {
      it("should be able to delete the existing key") {
        val id = UUID.randomUUID().toString
        api.set(id, "a").futureValue
        assert(api.del(Seq(id)).futureValue == 1)
      }
      it("shouldn't be able to delete the not existing key") {
        val id = UUID.randomUUID().toString
        assert(api.del(Seq(id)).futureValue == 0)
      }
    }
    describe("type") {
      describe("should be able to get the type of the registration key") {
        it("the string type") {
          val id = UUID.randomUUID().toString
          api.set(id, "a").futureValue
          assert(api.`type`(id).futureValue == ValueType.String)
        }
      }
    }
    describe("keys") {
      it("should be able to find the registration keys") {
        val id = UUID.randomUUID().toString
        api.set(id, "a").futureValue
        assert(api.keys("*").futureValue.contains(id))
      }
    }
    describe("ramdomkey") {
      it("should be able to generate the random key, if the registration keys exists") {
        for {_ <- 1 to 10} {
          val id = UUID.randomUUID().toString
          api.set(id, "a").futureValue
        }
        val randomKey = api.randomKey.futureValue
        assert(api.exists(randomKey.get).futureValue)
        assert(randomKey.isDefined)
      }
      it("shouldn't be able to generate the random key, if the registration keys are nothing") {
        val randomKey = api.randomKey.futureValue
        assert(randomKey.isEmpty)
      }
    }
    describe("rename") {
      it("should be able to rename it, if the new id doesn't exist") {
        val id = UUID.randomUUID().toString
        val newId = UUID.randomUUID().toString
        api.set(id, "a").futureValue
        api.rename(id, newId).futureValue
        assert(api.exists(newId).futureValue)
      }
    }
    describe("renamenx") {
      it("should be able to rename it, if the new id doesn't exist") {
        val id = UUID.randomUUID().toString
        val newId = UUID.randomUUID().toString
        api.set(id, "a").futureValue
        assert(api.renameNx(id, newId).futureValue)
        assert(api.exists(newId).futureValue)
      }
      it("shouldn't be able to rename it, if the new id exist") {
        val id = UUID.randomUUID().toString
        val newId = UUID.randomUUID().toString
        api.set(id, "a").futureValue
        api.set(newId, "a").futureValue
        assert(!api.renameNx(id, newId).futureValue)
      }
    }
  }

}
