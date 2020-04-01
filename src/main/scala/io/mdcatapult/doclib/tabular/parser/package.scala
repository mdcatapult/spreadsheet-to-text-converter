package io.mdcatapult.doclib.tabular

package object parser {

  def escape(text: String, target: String = "\"", escape: String = """\"""): String =
    text.replaceAllLiterally(target, s"$escape$target")
}
