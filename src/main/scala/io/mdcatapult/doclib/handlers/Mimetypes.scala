package io.mdcatapult.doclib.handlers

import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.models.DoclibDoc

object Mimetypes {
  val knownMimetypes =
    Seq(
      """application/vnd\.lotus.*""".r,
      """application/vnd\.ms-excel.*""".r,
      """application/vnd\.openxmlformats-officedocument.spreadsheetml.*""".r,
      """application/vnd\.stardivision.calc""".r,
      """application/vnd\.sun\.xml\.calc.*""".r,
      """application/vnd\.oasis\.opendocument\.spreadsheet""".r,
    )

  case class MimetypeNotAllowed(doc: DoclibDoc, cause: Throwable = None.orNull)
    extends DoclibDocException(
      doc,
      s"Document: ${doc._id.toHexString} - Mimetype '${doc.mimetype}' not allowed'",
      cause
    )

  def validateMimetype(doc: DoclibDoc): Option[Boolean] = {
    if (knownMimetypes.exists(_.matches(doc.mimetype))) {
      Some(true)
    } else {
      throw Mimetypes.MimetypeNotAllowed(doc)
    }
  }
}
