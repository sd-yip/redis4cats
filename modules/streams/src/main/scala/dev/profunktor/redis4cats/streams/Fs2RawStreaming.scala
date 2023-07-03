/*
 * Copyright 2018-2021 ProfunKtor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.profunktor.redis4cats
package streams

import scala.concurrent.duration.Duration

import cats.effect.kernel._
import cats.syntax.functor._
import dev.profunktor.redis4cats.JavaConversions._
import dev.profunktor.redis4cats.effect.FutureLift
import dev.profunktor.redis4cats.streams.data.Boundary.{ Exclusive, Inclusive, Unbounded }
import dev.profunktor.redis4cats.streams.data._
import dev.profunktor.redis4cats.streams.data.StreamingOffset.{ All, Custom, Latest }

import io.lettuce.core.{ Limit, Range, StreamMessage, XAddArgs, XReadArgs }
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.api.StatefulRedisConnection

private[streams] object RedisRawStreaming {
  private val toRangeBoundary: Boundary[MessageId] => Range.Boundary[String] = {
    case Unbounded     => Range.Boundary.unbounded()
    case Inclusive(id) => Range.Boundary.including(id.value)
    case Exclusive(id) => Range.Boundary.excluding(id.value)
  }

  private def toXReadMessages[K, V](list: java.util.List[StreamMessage[K, V]]) =
    list.asScala.toList.map { msg =>
      XReadMessage[K, V](MessageId(msg.getId), msg.getStream, msg.getBody.asScala.toMap)
    }
}

private[streams] class RedisRawStreaming[F[_]: FutureLift: Sync, K, V](
    val client: StatefulRedisConnection[K, V]
) extends RawStreaming[F, K, V] {
  import RedisRawStreaming._

  override def xAdd(key: K, body: Map[K, V], id: Option[MessageId], approxMaxlen: Option[Long]): F[MessageId] =
    FutureLift[F]
      .lift {
        val args = (id, approxMaxlen) match {
          case (None, None)       => null
          case (None, Some(n))    => new XAddArgs().maxlen(n).approximateTrimming(true)
          case (Some(i), None)    => new XAddArgs().id(i.value)
          case (Some(i), Some(n)) => new XAddArgs().id(i.value).maxlen(n).approximateTrimming(true)
        }
        client.async().xadd(key, args, body.asJava)
      }
      .map(MessageId.apply)

  override def xRange(
      key: K,
      start: Boundary[MessageId],
      end: Boundary[MessageId],
      count: Option[Long],
      isAscending: Boolean,
  ): F[List[XReadMessage[K, V]]] =
    FutureLift[F]
      .lift {
        val range = Range.from(toRangeBoundary(start), toRangeBoundary(end))
        val limit = count.fold(Limit.unlimited)(Limit.from)
        if (isAscending) client.async().xrange(key, range, limit) else client.async().xrevrange(key, range, limit)
      }
      .map(toXReadMessages)

  override def xRead(
      streams: Set[StreamingOffset[K]],
      block: Option[Duration] = Some(Duration.Zero),
      count: Option[Long] = None
  ): F[List[XReadMessage[K, V]]] =
    FutureLift[F]
      .lift {
        val offsets = streams.map {
          case All(key)            => StreamOffset.from(key, "0")
          case Latest(key)         => StreamOffset.latest(key)
          case Custom(key, offset) => StreamOffset.from(key, offset)
        }.toSeq

        (block, count) match {
          case (None, None)        => client.async().xread(offsets: _*)
          case (None, Some(count)) => client.async().xread(XReadArgs.Builder.count(count), offsets: _*)
          case (Some(block), None) => client.async().xread(XReadArgs.Builder.block(block.toMillis), offsets: _*)
          case (Some(block), Some(count)) =>
            client.async().xread(XReadArgs.Builder.block(block.toMillis).count(count), offsets: _*)
        }
      }
      .map(toXReadMessages)

}
