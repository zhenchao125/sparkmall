package com.atguigu.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/*
输入: (品类 id, 操作类型)
输出: Map((品类 id, 操作类型), 数量)
 */
class MapAccumulator extends AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] {
    val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {
        val newAcc = new MapAccumulator
        map.synchronized {
            newAcc.map ++= map
        }
        newAcc
    }

    override def reset(): Unit = map.clear


    override def add(v: (String, String)): Unit = {
        map(v) = map.getOrElseUpdate(v, 0) + 1
    }

    // otherMap: (1, click) -> 20  this: (1, click) -> 10         thisMap: (1,2) -> 30
    // otherMap: (1, order) -> 5                                  thisMap: (1,3) -> 5
    override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long]]): Unit = {
        val otherMap: mutable.Map[(String, String), Long] = other.value
        otherMap.foreach {
            //            kv => map.put(kv._1, map.getOrElse(kv._1, 0L) + kv._2)
            case (k, count) => {
                map.put(k, map.getOrElse(k, 0L) + count)
            }
        }
    }

    override def value: mutable.Map[(String, String), Long] = map
}
