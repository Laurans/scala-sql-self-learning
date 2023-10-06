package learnscala.introscala.shapes

@main def ProcessShapesDriver = 
    val messages = Seq(
        Draw(Circle(Point(0.0, 0.0), 1.0)),
        Draw(Rectangle(Point(0.0, 0.0), 2, 5)),
        Response(s"Say hello to pi: 3.14159"),
        Draw(Triangle(Point(0.0, 0.0), Point(2.0, 0.0), Point(1.0, 2.0))),
        Exit()
    )

    messages.foreach{ message =>
        val response = ProcessMessages(message)
        println(response)
    }