package com.github.j5ik2o.reactive.redis.command.keys
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.keys.ObjectRequest.SubCommand
import com.github.j5ik2o.reactive.redis.command.{
  CommandRequest,
  CommandRequestSupoprt,
  CommandResponse,
  StringParsersSupport
}
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

object ObjectRequest extends CommandRequestSupoprt {

  sealed trait SubCommand {
    val key: String
    val asString: String
  }

  private val commandName = "OBJECT"

  final case class RefCount(key: String) extends SubCommand {
    override val asString: String = cs(commandName, Some("REFCOUNT"), Some(key))
  }
  final case class Encoding(key: String) extends SubCommand {
    override val asString: String = cs(commandName, Some("ENCODING"), Some(key))
  }
  final case class IdleTime(key: String) extends SubCommand {
    override val asString: String = cs(commandName, Some("IDLETIME"), Some(key))
  }
  final case class Freq(key: String) extends SubCommand {
    override val asString: String = cs(commandName, Some("FREQ"), Some(key))
  }
}

final case class ObjectRequest(id: UUID, subCommand: SubCommand) extends CommandRequest with StringParsersSupport {

  override type Response = ObjectResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = subCommand.asString

  override protected def responseParser: P[Expr] = fastParse(integerReply | bulkStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (ObjectIntegerSucceeded(UUID.randomUUID(), id, n), next)
    case (StringOptExpr(s), next) =>
      (ObjectStringSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (ObjectSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ObjectFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }
}

sealed trait ObjectResponse                                                     extends CommandResponse
final case class ObjectSuspended(id: UUID, requestId: UUID)                     extends ObjectResponse
sealed abstract class ObjectSucceeded(id: UUID, requestId: UUID)                extends ObjectResponse
final case class ObjectIntegerSucceeded(id: UUID, requestId: UUID, value: Long) extends ObjectSucceeded(id, requestId)
final case class ObjectStringSucceeded(id: UUID, requestId: UUID, value: Option[String])
    extends ObjectSucceeded(id, requestId)
final case class ObjectFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends ObjectResponse
