package com.github.j5ik2o.reactive.redis.server

import com.github.j5ik2o.reactive.redis.CommandResponseParser._
import com.github.j5ik2o.reactive.redis._

object ServerProtocol {

  // --- BGREWRITEAOF
  // --- BGSAVE
  // --- CLIENT GETNAME
  // --- CLIENT KILL
  // --- CLIENT LIST
  // --- CLIENT PAUSE
  // --- CLIENT REPLY
  // --- CLIENT SETNAME
  // --- COMMAND
  // --- COMMAND COUNT
  // --- COMMAND GETKEYS
  // --- COMMAND INFO
  // --- CONFIG GET
  // --- CONFIG RESETSTAT
  // --- CONFIG REWRITE
  // --- CONFIG SET

  // --- DBSIZE
  case object DBSizeRequest extends CommandRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[DBSizeResponse] = {
        numberWithCrLfOrErrorWithCrLf ^^ {
          case NumberExpr(n) =>
            responseAsSucceeded(n)
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override type ResultType = Int

    override type ResponseType = DBSizeResponse

    override val encodeAsString: String = "DBSIZE"

    override def responseAsSucceeded(arguments: Int): DBSizeResponse = DBSizeSucceeded(arguments)

    override def responseAsFailed(ex: Exception): DBSizeResponse = DBSizeFailed(ex)

    override lazy val parser: CommandResponseParser[DBSizeResponse] = new Parser()
  }

  sealed trait DBSizeResponse extends CommandResponse

  case class DBSizeSucceeded(value: Int) extends DBSizeResponse

  case class DBSizeFailed(ex: Exception) extends DBSizeResponse

  // --- DEBUG OBJECT
  // --- DEBUG SEGFAULT

  // --- FLUSHALL
  case object FlushAllRequest extends CommandRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[FlushAllResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override def encodeAsString: String = "FLUSHALL"

    override type ResultType = Unit
    override type ResponseType = FlushAllResponse

    override def responseAsSucceeded(arguments: Unit): FlushAllResponse =
      FlushAllSucceeded

    override def responseAsFailed(ex: Exception): FlushAllResponse =
      FlushAllFailed(ex)

    override val parser: CommandResponseParser[FlushAllResponse] = new Parser()
  }

  sealed trait FlushAllResponse extends CommandResponse

  case object FlushAllSucceeded extends FlushAllResponse

  case class FlushAllFailed(ex: Exception) extends FlushAllResponse

  // --- FLUSHDB
  case object FlushDBRequest extends CommandRequest {
    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[FlushDBResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }
    override def encodeAsString: String = "FLUSHDB"

    override type ResultType = Unit
    override type ResponseType = FlushDBResponse

    override def responseAsSucceeded(arguments: Unit): FlushDBResponse =
      FlushDBSucceeded

    override def responseAsFailed(ex: Exception): FlushDBResponse =
      FlushDBFailed(ex)

    override val parser: CommandResponseParser[FlushDBResponse] = new Parser()
  }

  sealed trait FlushDBResponse extends CommandResponse

  case object FlushDBSucceeded extends FlushDBResponse

  case class FlushDBFailed(ex: Exception) extends FlushDBResponse


  // --- INFO
  case object InfoRequest extends CommandRequest {
    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[InfoResponse] = {
        bulkStringWithCrLf ^^ {
          case StringExpr(value) =>
            responseAsSucceeded(value)
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }
    override def encodeAsString: String = "INFO"

    override type ResultType = String
    override type ResponseType = InfoResponse

    override def responseAsSucceeded(arguments: String): InfoResponse =
      InfoSucceeded(arguments)

    override def responseAsFailed(ex: Exception): InfoResponse =
      InfoFailed(ex)

    override val parser: CommandResponseParser[InfoResponse] = new Parser()
  }

  sealed trait InfoResponse extends CommandResponse

  case class InfoSucceeded(value: String) extends InfoResponse

  case class InfoFailed(ex: Exception) extends InfoResponse

  // --- LASTSAVE
  // --- MONITOR
  // --- ROLE
  // --- SAVE
  // --- SHUTDOWN
  case class ShutdownRequest(save: Boolean) extends CommandRequest {
    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ShutdownResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }
    val saveOption = if (save) "SAVE" else "NOSAVE"
    override def encodeAsString: String = s"SHUTDOWN $saveOption"

    override type ResultType = Unit
    override type ResponseType = ShutdownResponse

    override def responseAsSucceeded(arguments: Unit): ShutdownResponse =
      ShutdownSucceeded

    override def responseAsFailed(ex: Exception): ShutdownResponse =
      ShutdownFailed(ex)

    override val parser: CommandResponseParser[ShutdownResponse] = new Parser()
  }

  sealed trait ShutdownResponse extends CommandResponse

  case object ShutdownSucceeded extends ShutdownResponse

  case class ShutdownFailed(ex: Exception) extends ShutdownResponse

  // --- SLAVEOF
  // --- SLOWLOG
  // --- SYNC
  // --- TIME
  case object TimeRequest extends CommandRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[TimeResponse] = {
        numberArrayWithCrLf ^^ {
          case ArrayExpr(values) =>
            responseAsSucceeded((values(0).value, values(1).value))
        }
      }
    }

    override def encodeAsString: String = "TIME"

    override type ResultType = (Int, Int)
    override type ResponseType = TimeResponse

    override def responseAsSucceeded(arguments: (Int, Int)): TimeResponse =
      TimeSucceeded(arguments._1, arguments._2)

    override def responseAsFailed(ex: Exception): TimeResponse =
      TimeFailed(ex)

    override val parser: CommandResponseParser[TimeResponse] = new Parser()
  }

  sealed trait TimeResponse extends CommandResponse

  case class TimeSucceeded(unixTime: Int, millis: Int) extends TimeResponse

  case class TimeFailed(ex: Exception) extends TimeResponse

}
