package ru.otus.homework

import ru.otus.homework.data.kafka.Consumer

object Main extends App {

  println("starting")
  val ddd = Consumer.readLogs()
  ddd.foreach(println(_))
}
