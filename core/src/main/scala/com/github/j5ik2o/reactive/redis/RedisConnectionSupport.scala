package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import akka.event.Logging
import monix.eval.Task

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Try

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Recursion"
  )
)
trait RedisConnectionSupport {
  this: RedisConnection =>

  import RedisConnection._

  val system: ActorSystem

  protected lazy val log = Logging(system, this)

  protected def calculateDelay(
      restartCount: Int,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double
  ): FiniteDuration = {
    val rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor
    val calculatedDuration =
      Try(maxBackoff.min(minBackoff * math.pow(2, restartCount.toDouble)) * rnd).getOrElse(maxBackoff)
    calculatedDuration match {
      case f: FiniteDuration ⇒ f
      case _                 ⇒ maxBackoff
    }
  }

  protected def retryBackoff[A](source: Task[A],
                                maxRetries: Int,
                                retryCount: Int,
                                minBackoff: FiniteDuration,
                                maxBackoff: FiniteDuration,
                                randomFactor: Double): Task[A] = {
    source.onErrorHandleWith {
      case ex: RedisRequestException =>
        if (maxRetries > 0) {
          // Recursive call, it's OK as Monix is stack-safe
          log.debug("retry backoff = {}", maxRetries)

          retryBackoff(source, maxRetries - 1, retryCount + 1, minBackoff, maxBackoff, randomFactor)
            .delayExecution(calculateDelay(retryCount, minBackoff, maxBackoff, randomFactor))
        } else
          Task.raiseError(ex)
    }
  }

}
