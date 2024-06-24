/*
 * Copyright 2024 Medicines Discovery Catapult
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
