package learnscala.introscala.shapes


sealed trait Message
case class Draw(shape: Shape) extends Message
case class Response(message: String) extends Message
case class Exit() extends Message