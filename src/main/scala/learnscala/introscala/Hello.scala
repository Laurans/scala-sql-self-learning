package learnscala.introscala

@main def Hello(params: String*): Unit =
  val output = params.map(_.toUpperCase).mkString(" ")
  println(output)
