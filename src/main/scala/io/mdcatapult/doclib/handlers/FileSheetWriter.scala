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
