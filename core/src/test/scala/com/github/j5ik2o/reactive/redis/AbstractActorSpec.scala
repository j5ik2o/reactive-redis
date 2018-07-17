package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks

import scala.concurrent.duration._

case class MockConnectionPool(peerConfig: PeerConfig)(implicit system: ActorSystem)
    extends RedisConnectionPool[Task]() {
  val conn = RedisConnection(peerConfig)

  override def withConnectionM[T](reader: ReaderRedisConnection[Task, T]): Task[T] = reader(conn)

  override def borrowConnection: Task[RedisConnection] = Task.pure(conn)

  override def returnConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

  def invalidateConnection(redisConnection: RedisConnection): Task[Unit] = Task.pure(())

  override def numActive: Int = 1

  override def clear(): Unit = {}

  override def dispose(): Unit = conn.shutdown()
}

abstract class AbstractActorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TimeFactorSupport
    with ScalaFutures
    with PropertyChecks
    with RedisSpecSupport
    with ScalaCheckSupport {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(60 * timeFactor seconds)

  implicit val materializer = ActorMaterializer()

  implicit val timeout = Timeout(15 seconds)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

//  protected def createConnectionPool(peerConfig: ConnectionConfig): RedisConnectionPool[Task] =
//    MockConnectionPool(connectionConfig)

  protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10, peerConfigs, newConnection = {
      RedisConnection(_)
    }, resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

}
