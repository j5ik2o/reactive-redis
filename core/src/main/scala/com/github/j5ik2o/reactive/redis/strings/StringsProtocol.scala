package com.github.j5ik2o.reactive.redis.strings

import com.github.j5ik2o.reactive.redis._

object StringsProtocol {

  import BaseProtocol._

  // --- SET ------------------------------------------------------------------------------------------------------

  trait SetRequest extends CommandRequest {
    val key: String
    val value: String
  }

  object SetRequest {

    def apply(key: String, value: String): SetRequest = new SetRequestImpl(key, value)

    def unapply(self: SetRequest): Option[(String, String)] = Some(self.key, self.value)

    private class SetRequestImpl(val key: String, val value: String) extends SetRequest {

      override def encodeAsString: String = s"SET $key $value"

      override type ResultType = Unit

      override type ResponseType = SetResponse

      override def responseAsSucceeded(arguments: Unit): SetResponse =
        SetSucceeded

      override def responseAsFailed(ex: Exception): SetResponse =
        SetFailed(ex)

      override val parser: CommandResponseParser[SetResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait SetResponse extends CommandResponse

  case object SetSucceeded extends SetResponse

  case class SetFailed(ex: Exception) extends SetResponse

  // --- GET ------------------------------------------------------------------------------------------------------

  trait GetRequest extends CommandRequest {
    val key: String
  }

  object GetRequest {

    def apply(key: String): GetRequest = new GetRequestImpl(key)

    def unapply(self: GetRequest): Option[String] = Some(self.key)

    private class GetRequestImpl(val key: String) extends GetRequest {

      override def encodeAsString: String = s"GET $key"

      override type ResultType = Option[String]

      override type ResponseType = GetResponse

      override def responseAsSucceeded(arguments: Option[String]): GetResponse =
        GetSucceeded(arguments)

      override def responseAsFailed(ex: Exception): GetResponse =
        GetFailure(ex)

      override val parser: CommandResponseParser[GetResponse] = new BulkStringParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait GetResponse extends CommandResponse

  case class GetSucceeded(value: Option[String]) extends GetResponse

  case class GetFailure(ex: Exception) extends GetResponse

  // --- GETSET ---------------------------------------------------------------------------------------------------

  trait GetSetRequest extends CommandRequest {
    val key: String
    val value: String
  }

  object GetSetRequest {

    def apply(key: String, value: String): GetSetRequest = new GetSetRequestImpl(key, value)

    def unapply(self: GetSetRequest): Option[(String, String)] = Some((self.key, self.value))

    private class GetSetRequestImpl(val key: String, val value: String) extends GetSetRequest {

      override def encodeAsString: String = s"GETSET $key $value"

      override type ResultType = Option[String]

      override type ResponseType = GetSetResponse

      override def responseAsSucceeded(arguments: Option[String]): GetSetResponse =
        GetSetSucceeded(arguments)

      override def responseAsFailed(ex: Exception): GetSetResponse =
        GetSetFailure(ex)

      override val parser: CommandResponseParser[ResponseType] = new BulkStringParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait GetSetResponse extends CommandResponse

  case class GetSetSucceeded(value: Option[String]) extends GetSetResponse

  case class GetSetFailure(ex: Exception) extends GetSetResponse
}
