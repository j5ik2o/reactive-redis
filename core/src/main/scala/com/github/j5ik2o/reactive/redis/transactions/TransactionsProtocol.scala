package com.github.j5ik2o.reactive.redis.transactions

import com.github.j5ik2o.reactive.redis.{ BaseProtocol, CommandResponseParser, RedisIOException }

object TransactionsProtocol {

  import BaseProtocol._

  // --- MULTI --------------------------------------------------------------------------------------------------------

  trait MultiRequest extends CommandRequest

  object MultiRequest {

    def apply(): MultiRequest = MultiRequestImpl

    def unapply(self: MultiRequest): Option[Unit] = Some(())

    private object MultiRequestImpl extends MultiRequest {

      override def encodeAsString: String = "MULTI"

      override type ResultType = Unit

      override type ResponseType = MultiResponse

      override def responseAsSucceeded(arguments: Unit): MultiResponse = MultiSucceeded

      override def responseAsFailed(ex: Exception): MultiResponse = MultiFailed(ex)

      override val parser: CommandResponseParser[MultiResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait MultiResponse extends CommandResponse

  case object MultiSucceeded extends MultiResponse

  case class MultiFailed(ex: Exception) extends MultiResponse

  // --- EXEC --------------------------------------------------------------------------------------------------------

  trait ExecRequest extends CommandRequest

  object ExecRequest {

    def apply(): ExecRequest = ExecRequestImpl

    def unapply(self: ExecRequest): Option[Unit] = Some(())

    private object ExecRequestImpl extends ExecRequest {

      override def encodeAsString: String = "EXEC"

      override type ResultType = Unit

      override type ResponseType = ExecResponse

      override def responseAsSucceeded(arguments: Unit): ExecResponse = ExecSucceeded

      override def responseAsFailed(ex: Exception): ExecResponse = ExecFailed(ex)

      override val parser: CommandResponseParser[ExecResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait ExecResponse extends CommandResponse

  case object ExecSucceeded extends ExecResponse

  case class ExecFailed(ex: Exception) extends ExecResponse

  // --- DISCARD --------------------------------------------------------------------------------------------------------

  trait DiscardRequest extends CommandRequest

  object DiscardRequest {

    def apply(): DiscardRequest = DiscardRequestImpl

    def unapply(self: DiscardRequest): Option[Unit] = Some(())

    private object DiscardRequestImpl extends DiscardRequest {

      override def encodeAsString: String = "DISCARD"

      override type ResultType = Unit

      override type ResponseType = DiscardResponse

      override def responseAsSucceeded(arguments: Unit): DiscardResponse = DiscardSucceeded

      override def responseAsFailed(ex: Exception): DiscardResponse = DiscardFailed(ex)

      override val parser: CommandResponseParser[DiscardResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait DiscardResponse extends CommandResponse

  case object DiscardSucceeded extends DiscardResponse

  case class DiscardFailed(ex: Exception) extends DiscardResponse

  // --- UNWATCH --------------------------------------------------------------------------------------------------------

  trait UnWatchRequest extends CommandRequest

  object UnWatchRequest {

    def apply(): UnWatchRequest = UnWatchRequestImpl

    def unapply(self: UnWatchRequest): Option[Unit] = Some(())

    private object UnWatchRequestImpl extends UnWatchRequest {

      override def encodeAsString: String = "UNWATCH"

      override type ResultType = Unit

      override type ResponseType = UnWatchResponse

      override def responseAsSucceeded(arguments: Unit): UnWatchResponse = UnWatchSucceeded

      override def responseAsFailed(ex: Exception): UnWatchResponse = UnWatchFailed(ex)

      override val parser: CommandResponseParser[UnWatchResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait UnWatchResponse extends CommandResponse

  case object UnWatchSucceeded extends UnWatchResponse

  case class UnWatchFailed(ex: Exception) extends UnWatchResponse

  // --- WATCH --------------------------------------------------------------------------------------------------------

  trait WatchRequest extends CommandRequest

  object WatchRequest {

    def apply(): WatchRequest = WatchRequestImpl

    def unapply(self: WatchRequest): Option[Unit] = Some(())

    private object WatchRequestImpl extends WatchRequest {

      override def encodeAsString: String = "WATCH"

      override type ResultType = Unit

      override type ResponseType = WatchResponse

      override def responseAsSucceeded(arguments: Unit): WatchResponse = WatchSucceeded

      override def responseAsFailed(ex: Exception): WatchResponse = WatchFailed(ex)

      override val parser: CommandResponseParser[WatchResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait WatchResponse extends CommandResponse

  case object WatchSucceeded extends WatchResponse

  case class WatchFailed(ex: Exception) extends WatchResponse

}
