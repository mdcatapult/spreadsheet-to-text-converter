package io.mdcatapult.doclib.tabular

package object parser {

  def escape(text: String, target: String = "\"", escape: String = "\\"): String =
    text.replace(target, s"$escape$target")
}
