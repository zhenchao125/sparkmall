package com.atguigu.sparkmall.offline

import java.text.DecimalFormat

object Test {
    def main(args: Array[String]): Unit = {
        val formater = new DecimalFormat("0.00")
        println(formater.format(1))
    }
}
