package io.mdcatapult.doclib.handlers

import io.mdcatapult.doclib.consumer.HandlerResult
import io.mdcatapult.doclib.models.DoclibDoc

case class AnyHandlerResult(doclibDoc: DoclibDoc) extends HandlerResult