package io.mdcatapult.doclib.messages

import io.mdcatapult.klein.queue.Envelope
import play.api.libs.json.{Format, Json, Reads, Writes}

object PrefetchMsg {
  implicit val msgReader: Reads[PrefetchMsg] = Json.reads[PrefetchMsg]
  implicit val msgWriter: Writes[PrefetchMsg] = Json.writes[PrefetchMsg]
  implicit val msgFormatter: Format[PrefetchMsg] = Json.format[PrefetchMsg]
}

case class PrefetchMsg(source: String, origin: String, tags: List[String]) extends Envelope


