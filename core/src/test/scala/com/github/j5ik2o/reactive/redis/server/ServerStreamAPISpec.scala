package com.github.j5ik2o.reactive.redis.server

import java.net.InetSocketAddress
import java.time.{ Instant, ZoneId, ZonedDateTime }

import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.server.ServerProtocol.{ DBSizeSucceeded, TimeSucceeded }
import com.github.j5ik2o.reactive.redis.{ ActorSpec, ServerBootable, StreamAPI }
import org.scalatest.time.{ Millis, Seconds, Span }

import scala.concurrent.Future

class ServerStreamAPISpec
  extends ActorSpec(ActorSystem("ServerStreamAPISpec"))
    with ServerBootable {


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
    //api.quit.futureValue
    system.terminate()
    super.afterAll()
  }

  describe("ServerStreamAPI") {
    // --- BGREWRITEAOF
    // --- BGSAVE
    describe("BGSAVE") {
      it("should be able to save data in the background") {
        api.run(api.set("a", "1")).futureValue
        api.run(api.bgSave).futureValue
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
        api.run(api.set("a", "1")).futureValue
        api.run(api.dbSize).futureValue match {
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
        api.run(api.flushAll).futureValue
      }
    }
    // --- FLUSHDB
    describe("FLUSHDB") {
      it("should be able to flush the db") {
        api.run(api.flushDB).futureValue
      }
    }

    // --- INFO
    describe("INFO") {
      it("should be able to get the information") {
        val result = api.run(api.info).futureValue
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
        val result = api.run(api.time).futureValue
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
        // api.run(api.shutdown()).futureValue
      }
    }

  }

}
