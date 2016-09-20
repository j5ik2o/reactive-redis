package com.github.j5ik2o.reactive.redis.server

import java.net.InetSocketAddress
import java.time.{ Instant, ZoneId, ZonedDateTime }

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.server.ServerProtocol.{ DBSizeSucceeded, TimeSucceeded }
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisAPIExecutor, ServerBootable }

class ServerStreamAPISpec
  extends ActorSpec(ActorSystem("ServerStreamAPISpec"))
    with ServerBootable {

  import com.github.j5ik2o.reactive.redis.RedisCommandRequests._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val address = new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)
    executor = RedisAPIExecutor(address)
  }

  override protected def afterAll(): Unit = {
    //api.quit.futureValue
    system.terminate()
    super.afterAll()
  }

  describe("ServerStreamAPI") {
    // --- BGREWRITEAOF
    // --- BGSAVE
    describe("BGSAVE") {
      it("should be able to save data in the background") {
        executor.execute(set("a", "1")).futureValue
        executor.execute(bgSaveRequest).futureValue
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
        executor.execute(set("a", "1")).futureValue
        executor.execute(dbSizeRequest).futureValue match {
          case Seq(DBSizeSucceeded(size)) =>
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
        executor.execute(flushAllRequest).futureValue
      }
    }
    // --- FLUSHDB
    describe("FLUSHDB") {
      it("should be able to flush the db") {
        executor.execute(flushDB).futureValue
      }
    }

    // --- INFO
    describe("INFO") {
      it("should be able to get the information") {
        val result = executor.execute(infoRequest).futureValue
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
        val result = executor.execute(time).futureValue
        result match {
          case Seq(TimeSucceeded(unixTime, millis)) =>
            val now = ZonedDateTime.now()
            val instant = Instant.ofEpochSecond(unixTime)
            val dateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault())
            assert(now.toInstant.getEpochSecond == dateTime.toInstant.getEpochSecond)
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
