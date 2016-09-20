package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.pattern.ask
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String._
import com.github.j5ik2o.reactive.redis.connection.ConnectionProtocol._
import com.github.j5ik2o.reactive.redis.keys.KeysProtocol._
import com.github.j5ik2o.reactive.redis.keys.ValueType
import org.scalatest.BeforeAndAfter

import scala.concurrent.Await
import scala.concurrent.duration._

class StringClientSpec
  extends ActorSpec(ActorSystem("StringClientSpec")) with ServerBootable
    with BeforeAndAfter {

//  override protected def beforeAll(): Unit = {
//    super.beforeAll()
//    val client = system.actorOf(StringClient.props(new InetSocketAddress("127.0.0.1", testServer.address.get.getPort)))
//    clientRef.set(client)
//  }
//
//  override protected def afterAll(): Unit = {
//    //Await.result(client ? QuitRequest, 10 seconds)
//    system.terminate()
//    super.afterAll()
//  }

  describe("StringClient") {
    it("should be set value") {
//      val id = UUID.randomUUID.toString
//      val value = UUID.randomUUID.toString
//      client ! SetRequest(id, value)
//      expectMsg(SetSucceeded)
//      client ! TypeRequest(id)
//      assert(expectMsgType[TypeSucceeded].value == ValueType.String)
    }
    it("should be got value") {
//      val id = UUID.randomUUID.toString
//      val value = UUID.randomUUID.toString
//      client ! SetRequest(id, value)
//      expectMsg(SetSucceeded)
//      client ! GetRequest(id)
//      assert(expectMsgType[GetSucceeded].value.contains(value))
    }
    it("should be got and set value") {
//      val id = UUID.randomUUID.toString
//      val value1 = UUID.randomUUID.toString
//      val value2 = UUID.randomUUID.toString
//      client ! SetRequest(id, value1)
//      expectMsg(SetSucceeded)
//      client ! GetSetRequest(id, value2)
//      val result2 = expectMsgType[GetSetSucceeded]
//      assert(result2.value == value1)
    }
  }

}
