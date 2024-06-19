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
