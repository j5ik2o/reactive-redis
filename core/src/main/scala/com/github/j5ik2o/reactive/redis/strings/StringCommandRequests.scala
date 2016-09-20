package com.github.j5ik2o.reactive.redis.strings

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String.{ GetRequest, GetSetRequest, SetRequest }

trait StringCommandRequests {

  def setRequest(key: String, value: String): Source[SetRequest, NotUsed] =
    Source.single(SetRequest(key, value))

  def getRequest(key: String): Source[GetRequest, NotUsed] =
    Source.single(GetRequest(key))

  def getSetRequest(key: String, value: String): Source[GetSetRequest, NotUsed] =
    Source.single(GetSetRequest(key, value))

}
