package io.mdcatapult.doclib.handlers

import io.mdcatapult.doclib.consumer.HandlerResult
import io.mdcatapult.doclib.models.DoclibDoc

case class SpreadSheetHandlerResult(doclibDoc: DoclibDoc,
                                    newPathsFromSpreadsheet: List[String]) extends HandlerResult