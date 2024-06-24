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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Path

import io.mdcatapult.doclib.tabular.Sheet

class FileSheetWriter(override val convertToFormat: String) extends SheetWriter {

  override def writeSheet(sheet: Sheet, targetPath: Path): Sheet = {
    val filename =
      sheet
        .name
        .replaceAll(" ", "_")
        .replaceAll("[^0-9a-zA-Z_-]", "-")

    val target = targetPath.resolve(s"${sheet.index}_$filename.$convertToFormat").toFile
    target.getParentFile.mkdirs()

    val fileWriter = new FileWriter(target)

    try {
      val w = new BufferedWriter(fileWriter)
      try {
        w.write(sheet.content)
      } finally {
        w.close()
      }
      sheet.copy(path = Some(target.getAbsolutePath))
    } finally {
      fileWriter.close()
    }
  }
}
