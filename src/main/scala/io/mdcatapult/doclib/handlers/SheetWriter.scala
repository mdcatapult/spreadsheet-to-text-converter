package io.mdcatapult.doclib.handlers

import java.nio.file.Path

import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}

trait SheetWriter {

  /** Extension of file that sheets are written to. */
  val convertToFormat: String

  /**
    * Persist to FS and return new sheet with new path and normalised filename.
    * @param sheet TabSheet
    * @param targetPath String
    * @return sheet with absolute path
    */
  def writeSheet(sheet: TabSheet, targetPath: Path): TabSheet
}
