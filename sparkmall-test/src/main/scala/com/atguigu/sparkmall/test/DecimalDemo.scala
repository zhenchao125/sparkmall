package com.atguigu.sparkmall.test

import java.text.DecimalFormat

object DecimalDemo {
    def main(args: Array[String]): Unit = {
        val format = new DecimalFormat(".00%")
        println(format.format(0.15558))
    }
}
