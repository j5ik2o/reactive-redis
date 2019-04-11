package com.github.j5ik2o.reactive.redis.feature

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis._
import org.scalacheck.Shrink

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import cats.implicits._

abstract class AbstractConnectionFeatureSpec extends AbstractRedisClientSpec(ActorSystem("ConnectionFeatureSpec")) {
  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  "ConnectionFeatureSpec" - {
    "ping without parameter" in {
      def stopAndStart(n: Int): Future[Unit] = {
        if (n == 0) Future.successful(())
        else {

          Thread.sleep((50 * timeFactor milliseconds).toMillis)
          redisMasterServer.stop()
          Thread.sleep((50 * timeFactor milliseconds).toMillis)
          redisMasterServer.start(Some(redisMasterServer.getPort))
          stopAndStart(n - 1)
        }
      }
      val f = stopAndStart(5)
      for { _ <- 1 to 100 } {
        val result = runProgram(for {
          r <- redisClient.ping()
        } yield r)
        result.value shouldBe "PONG"
      }
      Await.result(f, Duration.Inf)
    }
    "ping" in forAll(keyStrValueGen) {
      case (_, value) =>
        val result = runProgram(for {
          r <- redisClient.ping(Some(value))
        } yield r)
        result.value shouldBe value
    }
    "select" in {
      runProgram(for {
        _ <- redisClient.select(1)
      } yield ())
    }
//    "swapdb" in {
//      runProgram(
//        for {
//          _ <- redisClient.swapDB(0, 1)
//        } yield ()
//      )
//    }
//    "quit" in {
//      an[RedisRequestException] should be thrownBy {
//        runProgram(for {
//          _ <- {
//            val result = redisClient.quit()
//            Thread.sleep((3 * timeFactor seconds).toMillis)
//            result
//          }
//          r <- redisClient.set("aaaa", "bbbb")
//        } yield r)
//      }
//    }
  }
}
