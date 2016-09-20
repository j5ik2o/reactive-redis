package com.github.j5ik2o.reactive.redis.keys

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String.{ GetSucceeded, SetSucceeded }
import com.github.j5ik2o.reactive.redis.keys.KeysProtocol._
import com.github.j5ik2o.reactive.redis.{ ActorSpec, ServerBootable, StreamAPI }
import org.scalatest.DiagrammedAssertions

import scala.concurrent.Future

class KeysStreamAPISpec
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
    api.run(api.quit).futureValue
    system.terminate()
    super.afterAll()
  }

  describe("KeysStreamAPI") {
    // --- DEL
    describe("DEL") {
      it("should be able to delete the existing key") {
        val id = UUID.randomUUID().toString
        api.run(api.set(id, "a")).futureValue
        assert(api.run(api.del(Seq(id))).futureValue == Seq(DelSucceeded(1)))
      }
      it("shouldn't be able to delete the not existing key") {
        val id = UUID.randomUUID().toString
        assert(api.run(api.del(Seq(id))).futureValue == Seq(DelSucceeded(0)))
      }
    }
    //    // --- DUMP
    describe("DUMP") {
      it("should be able to dump key") {
        val id = UUID.randomUUID().toString
        api.run(api.set(id, "a")).futureValue
        assert(api.run(api.dump(id)).futureValue.head.asInstanceOf[DumpSucceeded].value.nonEmpty)
      }
      // --- EXISTS
      describe("EXISTS") {
        it("should be able to exist the registration key") {
          val id = UUID.randomUUID().toString
          api.run(api.set(id, "a")).futureValue
          assert(api.run(api.exists(id)).futureValue.head.asInstanceOf[ExistsSucceeded].value == 1)
        }
        it("shouldn't be able to exist the un-registration key") {
          val id = UUID.randomUUID().toString
          assert(api.run(api.exists(id)).futureValue.head.asInstanceOf[ExistsSucceeded].value == 0)
        }
      }
      // --- EXPIRE
      describe("EXPIRE") {
        it("should be able to expire data") {
          val id = UUID.randomUUID().toString
          api.run(api.set(id, "a")).futureValue
          assert(api.run(api.expire(id, 2)).futureValue.head.asInstanceOf[ExpireSucceeded].value == 1)
          Thread.sleep(3 * 1000)
          assert(api.run(api.get(id)).futureValue.head.asInstanceOf[GetSucceeded].value.isEmpty)
        }
      }
      //    // --- EXPIREAT
      //    describe("EXPIREAT") {
      //      it("should be able to expire data") {
      //        val id = UUID.randomUUID().toString
      //        api.set(id, "a").futureValue
      //        assert(api.expireAt(id, (System.currentTimeMillis() + 2000) / 1000).futureValue)
      //        Thread.sleep(3 * 1000)
      //        assert(api.get(id).futureValue.isEmpty)
      //      }
    }
    // --- KEYS
    describe("KEYS") {
      it("should be able to find the registration keys") {
        val id = UUID.randomUUID().toString
        assert(api.run(api.set(id, "a").concat(api.keys("*"))).futureValue == Seq(SetSucceeded, KeysSucceeded(Seq(id))))
      }
      it("shouldn't be able to find the unregistration keys") {
        val id = UUID.randomUUID().toString
        // api.run(api.set(id, "a")).futureValue
        assert(api.run(api.keys(id)).futureValue == Seq(KeysSucceeded(Seq.empty)))
      }
    }
    // --- MIGRATE

    // --- MOVE
    describe("MOVE") {
      it("should be able to move the key") {
        val id = UUID.randomUUID().toString
        import com.github.j5ik2o.reactive.redis.DSLSupport._
        val cmds = api.set(id, "a") and api.move(id, 1)
        api.run(cmds).futureValue
        assert(api.run(api.keys(id)).futureValue.head.asInstanceOf[KeysSucceeded].values.isEmpty)
      }
    }
    //
    //    // --- OBJECT
    //
    //    // --- PERSIST
    //
    //    // --- PEXPIRE
    //
    //    // --- PEXPIREAT
    //
    //    // --- PTTL
    //
    //    // --- RANDOMKEY
    describe("RANDOMKEY") {
      //      it("should be able to generate the random key, if the registration keys exists") {
      //        for {_ <- 1 to 10} {
      //          val id = UUID.randomUUID().toString
      //          api.set(id, "a").futureValue
      //        }
      //        val randomKey = api.randomKey.futureValue
      //        assert(api.exists(randomKey.get).futureValue)
      //        assert(randomKey.isDefined)
      //      }
      //      it("shouldn't be able to generate the random key, if the registration keys are nothing") {
      //        val randomKey = api.randomKey.futureValue
      //        assert(randomKey.isEmpty)
      //      }
      //    }
      //    // --- RENAME
      //    describe("RENAME") {
      //      it("should be able to rename it, if the new id doesn't exist") {
      //        val id = UUID.randomUUID().toString
      //        val newId = UUID.randomUUID().toString
      //        api.set(id, "a").futureValue
      //        api.rename(id, newId).futureValue
      //        assert(api.exists(newId).futureValue)
      //      }
      //    }
      //    // --- RENAMENX
      //    describe("RENAMENX") {
      //      it("should be able to rename it, if the new id doesn't exist") {
      //        val id = UUID.randomUUID().toString
      //        val newId = UUID.randomUUID().toString
      //        api.set(id, "a").futureValue
      //        assert(api.renameNx(id, newId).futureValue)
      //        assert(api.exists(newId).futureValue)
      //      }
      //      it("shouldn't be able to rename it, if the new id exist") {
      //        val id = UUID.randomUUID().toString
      //        val newId = UUID.randomUUID().toString
      //        api.set(id, "a").futureValue
      //        api.set(newId, "a").futureValue
      //        assert(!api.renameNx(id, newId).futureValue)
      //      }
      //    }
      //
      //    // --- RESTORE
      //
      //    // --- SCAN
      //
      //    // --- SORT
      //
      // --- TTL
      describe("TTL") {
        it("should be able to get TTL of registration key") {
          //              val id = UUID.randomUUID().toString
          //              api.run(api.set(id, "a")).futureValue
          //              assert(api.run(api.expire(id, 2)).futureValue)
          //              assert(api.run(api.ttl(id)).futureValue > 0)
          //              Thread.sleep(3 * 1000)
          //              assert(api.run(api.get(id)).futureValue.isEmpty)
        }
      }
      // --- TYPE
      describe("TYPE") {
        describe("should be able to get the type of the registration key") {
          it("the string type") {
            val id = UUID.randomUUID().toString
            api.run(api.set(id, "a")).futureValue
            assert(api.run(api.`type`(id)).futureValue == Seq(TypeSucceeded(ValueType.String)))
          }
        }
      }

      // --- WAIT


    }

  }
}