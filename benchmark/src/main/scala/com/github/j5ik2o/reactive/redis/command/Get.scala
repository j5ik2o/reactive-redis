package com.github.j5ik2o.reactive.redis.command

import java.util.concurrent.TimeUnit

import cats.implicits._
import com.github.j5ik2o.reactive.redis.BenchmarkHelper
import monix.eval.Task
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class Get extends BenchmarkHelper {

  override def fixture(): Unit = {
    Await.result(reactiveRedisPoolOfJedisQueue.withConnectionF { con =>
      client.set("A", "value").run(con)
    }.runToFuture, Duration.Inf)
  }

  @Benchmark
  def reactiveRedisOfDefaultQueue(): Unit = {
    Await.result(reactiveRedisPoolOfDefaultQueue.withConnectionF { con =>
      client.get("A").run(con)
    }.runToFuture, Duration.Inf)
    ()
  }

  @Benchmark
  def reactiveRedisOfDefaultActor(): Unit = {
    Await.result(reactiveRedisPoolOfDefaultActor.withConnectionF { con =>
      client.get("A").run(con)
    }.runToFuture, Duration.Inf)
    ()
  }

  @Benchmark
  def reactiveRedisOfJedisQueue(): Unit = {
    Await.result(reactiveRedisPoolOfJedisQueue.withConnectionF { con =>
      client.get("A").run(con)
    }.runToFuture, Duration.Inf)
    ()
  }

  @Benchmark
  def reactiveRedisOfJedisActor(): Unit = {
    Await.result(reactiveRedisPoolOfJedisActor.withConnectionF { con =>
      client.get("A").run(con)
    }.runToFuture, Duration.Inf)
    ()
  }

  @Benchmark
  def jedis: Unit = {
    Await.result(Task {
      val jedis = jedisPool.getResource
      jedis.get("A")
      jedis.close()
    }.runToFuture, Duration.Inf)
    ()
  }

  @Benchmark
  def rediscala(): Unit = {
    Await.result(rediscalaPool.get("A"), Duration.Inf)
    ()
  }

  @Benchmark
  def scalaRedis(): Unit = {
    Await.result(Task {
      scalaRedisPool.withClient { client =>
        client.get("A")
      }
    }.runToFuture, Duration.Inf)
    ()
  }

  @Setup(Level.Trial)
  override def setup(): Unit = super.setup()

  @TearDown(Level.Trial)
  override def tearDown(): Unit = super.tearDown()
}
