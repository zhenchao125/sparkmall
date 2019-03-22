package com.atguigu.sparkmall.test

object SortByDemo {
    implicit object PersonOrdering extends Ordering[Person]{
        override def compare(x: Person, y: Person): Int = x.age - y.age
    }

    def main(args: Array[String]): Unit = {

//        val list = List(10, 20, 1, 4, 8, 30)
//        println(list.sorted(Ordering.Int.reverse))
        val list = List(Person(10, "zs"), Person(20, "ls"), Person(15, "ww"))
        println(list.sorted(PersonOrdering.reverse))
    }
}

case class Person(age: Int, name: String)
