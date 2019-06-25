package io.mdcatapult.doclib.tabular

case class Sheet(
                index: Int,
                name: String,
                content: String,
                path: Option[String] = None
                ) {

}
