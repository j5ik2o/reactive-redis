package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks

import scala.concurrent.duration._

abstract class ActorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TimeFactorSupport
    with ScalaFutures
    with PropertyChecks
    with RedisSpecSupport {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(30 * timeFactor seconds)

  implicit val materializer = ActorMaterializer()

  implicit val timeout = Timeout(15 seconds)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    shutdown()
    super.beforeAll()
  }

}
