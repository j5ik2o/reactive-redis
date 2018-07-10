package com.github.j5ik2o.reactive.redis

import cats.MonadError
import cats.conts.ContT
import com.github.j5ik2o.reactive.redis.command.CommandResponse

object CommandResponseCont {

  def apply[M[_], A](
      f: (A => M[CommandResponse]) => M[CommandResponse]
  )(implicit F: MonadError[M, Throwable]): ContT[M, CommandResponse, A] = ContT { v =>
    f(v)
  }

  def fromM[M[_], A](task: => M[A])(implicit F: MonadError[M, Throwable]): ContT[M, CommandResponse, A] =
    ContT(F.flatMap(task))

  def successful[M[_], A](a: A)(implicit F: MonadError[M, Throwable]): ContT[M, CommandResponse, A] = fromM(F.pure(a))

  def failed[M[_], A](throwable: Throwable)(implicit F: MonadError[M, Throwable]): ContT[M, CommandResponse, A] =
    fromM(F.raiseError(throwable))

}
