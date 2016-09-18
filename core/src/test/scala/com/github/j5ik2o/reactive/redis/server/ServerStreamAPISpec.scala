package com.github.j5ik2o.reactive.redis.server

import java.net.InetSocketAddress
import java.time.{ Instant, ZoneId, ZonedDateTime }

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
    //api.quit.futureValue
    system.terminate()
    super.afterAll()
  }

  describe("ServerStreamAPI") {
    // --- BGREWRITEAOF
    // --- BGSAVE
    describe("BGSAVE") {
      it("should be able to save data in the background") {
        api.set("a", "1").futureValue
        api.bgSave.futureValue
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
        api.set("a", "1").futureValue
        assert(api.dbSize.futureValue > 0)
      }
    }
    // --- DEBUG OBJECT
    // --- DEBUG SEGFAULT

    // --- FLUSHALL
    describe("FLUSHALL") {
      it("should be able to flush all dbs") {
        api.flushAll.futureValue
      }
    }
    // --- FLUSHDB
    describe("FLUSHDB") {
      it("should be able to flush the db") {
        api.flushDB.futureValue
      }
    }

    // --- INFO
    describe("INFO") {
      it("should be able to get the information") {
        val result = api.info.futureValue
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
        val result = api.time.futureValue
        val now = ZonedDateTime.now()
        val dateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(result(0)), ZoneId.systemDefault())
        assert(now.toInstant.getEpochSecond == dateTime.toInstant.getEpochSecond)
      }
    }

    // --- SHUTDOWN
    describe("SHUTDOWN") {
      it("should be able to shut down the server") {
        api.shutdown().futureValue
      }
    }

  }

}
