package com.github.j5ik2o.reactive.redis.command

import java.util.concurrent.TimeUnit

import cats.implicits._
import com.github.j5ik2o.reactive.redis.{ BenchmarkHelper, Result }
import org.openjdk.jmh.annotations._

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
class Get extends BenchmarkHelper {
  @Param(Array("1000", "5000"))
  var iteration: Int = _

  var getKey = "getKey"

  override def fixture(): Unit = {
    Await.result(pool.withConnectionF { con =>
      client.set(getKey, "value").run(con)
    }.runAsync, 20 seconds)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def rediscala(): Unit = {
    val futures = for (i <- (0 to iteration).toVector) yield {
      rediscalaPool.get(getKey)
    }
    Await.result(Future.sequence(futures), Duration.Inf)
    ()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def reactiveRedis(): Unit = {
    val futures: Seq[Future[Result[Option[String]]]] = for (i <- (0 to iteration).toVector) yield {
      pool.withConnectionF { con =>
        client.get(getKey).run(con)
      }.runAsync
    }
    Await.result(Future.sequence(futures), Duration.Inf)
    ()
  }

}
