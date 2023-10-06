package learnscala.introscala.shapes

object ProcessMessages:
    def apply(message: Message): Message = 
        message match
            case Exit() =>
                println("Process message: Exiting ")
                Exit()
            case Draw(shape) =>
                shape.draw(str => println(s"Process message: $str"))
                Response(s"Process message: $shape drawn")
            case Response(unexpected) =>
                val response = Response(s"This is unexpected $unexpected")
                println(s"Process message: $response")
                response
        

