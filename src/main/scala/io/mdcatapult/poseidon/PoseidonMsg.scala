package io.mdcatapult.poseidon

import io.mdcatapult.klein.queue.Envelope
import play.api.libs.json.{Format, Json, Reads, Writes}


object PoseidonMsg {
  implicit val msgReader: Reads[PoseidonMsg] = Json.reads[PoseidonMsg]
  implicit val msgWriter: Writes[PoseidonMsg] = Json.writes[PoseidonMsg]
  implicit val msgFormatter: Format[PoseidonMsg] = Json.format[PoseidonMsg]
}

case class PoseidonMsg(id: String, config: Option[String] = None) extends Envelope


