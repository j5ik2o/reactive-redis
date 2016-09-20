package com.github.j5ik2o.reactive.redis.server

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String.SetRequest
import com.github.j5ik2o.reactive.redis.server.ServerProtocol.{ BgSaveSucceeded, DBSizeRequest, DBSizeSucceeded, TimeSucceeded }
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisAPIExecutor, ServerBootable }

class ServerStreamAPISpec
    extends ActorSpec(ActorSystem("ServerStreamAPISpec"))
    with ServerBootable {

  import com.github.j5ik2o.reactive.redis.RedisCommandRequests._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    executor = Some(RedisAPIExecutor(address))
  }

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  describe("ServerStreamAPI") {
    // --- BGREWRITEAOF
    // --- BGSAVE
    describe("BGSAVE") {
      it("should be able to save data in the background") {
        executor.foreach(_.execute(setRequest("a", "1")).futureValue)
        assert(executor.map(_.execute(bgSaveRequest).futureValue).get.head.isInstanceOf[BgSaveSucceeded.type])
      }
    }
    // --- CLIENT GETNAME
    // --- CLIENT KILL
    // --- CLIENT LIST
    // --- CLIENT PAUSE
    // --- CLIENT REPLY
    // --- CLIENT SETNAME
    // --- COMMAND
    // --- COMMAND COUNT
    // --- COMMAND GETKEYS
    // --- COMMAND INFO
    // --- CONFIG GET
    // --- CONFIG RESETSTAT
    // --- CONFIG REWRITE
    // --- CONFIG SET

    // --- DBSIZE
    describe("DBSIZE") {
      it("should be able to get the size of the db") {
        executor.map(_.execute(setRequest("a", "1") ++ dbSizeRequest).futureValue).get match {
          case Seq(_, DBSizeSucceeded(size)) =>
            assert(size > 0)
          case _ =>
            fail()
        }
      }
    }
    // --- DEBUG OBJECT
    // --- DEBUG SEGFAULT

    // --- FLUSHALL
    describe("FLUSHALL") {
      it("should be able to flush all dbs") {
        executor.foreach(_.execute(flushAllRequest).futureValue)
      }
    }
    // --- FLUSHDB
    describe("FLUSHDB") {
      it("should be able to flush the db") {
        executor.foreach(_.execute(flushDB).futureValue)
      }
    }

    // --- INFO
    describe("INFO") {
      it("should be able to get the information") {
        val result = executor.map(_.execute(infoRequest).futureValue).get
        result.foreach(println)
        assert(result.nonEmpty)
      }
    }
    // --- LASTSAVE
    // --- MONITOR
    // --- ROLE
    // --- SAVE
    // --- SLAVEOF
    // --- SLOWLOG
    // --- SYNC
    // --- TIME
    describe("TIME") {
      it("should be able to get time on the server") {
        val result = executor.map(_.execute(time).futureValue).get
        result match {
          case Seq(TimeSucceeded(unixTime, millis)) =>
            assert(unixTime > 0 && millis > 0)
        }
      }
    }

    // --- SHUTDOWN
    describe("SHUTDOWN") {
      it("should be able to shut down the server") {
        // executor.execute(api.shutdown()).futureValue
      }
    }

  }

}
