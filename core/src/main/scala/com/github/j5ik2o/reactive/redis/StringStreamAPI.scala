package com.github.j5ik2o.reactive.redis

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String.{ GetRequest, GetSetRequest, SetRequest }

trait StringStreamAPI extends BaseStreamAPI {

  def set(key: String, value: String): Source[SetRequest, NotUsed] =
    Source.single(SetRequest(key, value))

  def get(key: String): Source[GetRequest, NotUsed] =
    Source.single(GetRequest(key))

  def getSet(key: String, value: String): Source[GetSetRequest, NotUsed] =
    Source.single(GetSetRequest(key, value))

}
