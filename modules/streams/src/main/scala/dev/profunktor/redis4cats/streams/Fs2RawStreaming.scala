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
import dev.profunktor.redis4cats.streams.data._
import dev.profunktor.redis4cats.streams.data.StreamingOffset.{ All, Custom, Latest }

import io.lettuce.core.{ XAddArgs, XReadArgs }
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.api.StatefulRedisConnection

private[streams] class RedisRawStreaming[F[_]: FutureLift: Sync, K, V](
    val client: StatefulRedisConnection[K, V]
) extends RawStreaming[F, K, V] {

  override def xAdd(key: K, body: Map[K, V], id: Option[MessageId], approxMaxlen: Option[Long]): F[MessageId] =
    FutureLift[F]
      .lift {
        var args: XAddArgs = null

        if (approxMaxlen.isDefined || id.isDefined) {
          args = new XAddArgs()
          id.foreach(i => args.id(i.value))
          approxMaxlen.foreach(args.maxlen(_).approximateTrimming(true))
        }
        client.async().xadd(key, args, body.asJava)
      }
      .map(MessageId.apply)

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
      .map { list =>
        list.asScala.toList.map { msg =>
          XReadMessage[K, V](MessageId(msg.getId), msg.getStream, msg.getBody.asScala.toMap)
        }
      }

}
