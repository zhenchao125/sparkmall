package com.atguigu.sparkmall.offline.udf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityClickCountUDAF extends UserDefinedAggregateFunction {


    // 输入数据的类型 北京:String
    override def inputSchema: StructType = {
        StructType(StructField("city_name", StringType) :: Nil)
    }

    // 存储类型 map[北京, count]  总点击数:total_count
    override def bufferSchema: StructType = {
        StructType(StructField("city_count", MapType(StringType, LongType)) :: StructField("total_count", LongType) :: Nil)
    }

    // 输出的数据类型  String
    override def dataType: DataType = StringType

    // 校验 相同输入是否有相同的输出 true
    override def deterministic: Boolean = true

    // 初始化  给 map 和 totalCount 赋初值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 初始化 map
        buffer(0) = Map[String, Long]()
        // 初始化总值
        buffer(1) = 0L
    }

    // 分区执行更新操作.  executor 内的合并
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val cityName: String = input.getString(0)
        val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
        // 更新各个城市的计数
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
        // 更新总数
        buffer(1) = buffer.getLong(1) + 1
    }

    // 多个 buffer 合并. 跨 executor 合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // 合并各个城市的累计
        val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
        val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
        // 合并每个城市的点击量
        buffer1(0) = map1.foldLeft(map2) {
            case (m, (cityName, cityCount)) => {
                m + (cityName -> (m.getOrElse(cityName, 0L) + cityCount))
            }
        }
        // 合并总数
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 最终的输出  把 buffer 中的数据展示成字符串
    override def evaluate(buffer: Row): Any = {
        // 1. 取值
        val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
        val totalCount = buffer.getLong(1)
        // 2. 排序之后截取前2个, 并统计他们的比值
        var cityTop2: List[(String, Long)] = map.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
        var cityRemarks: List[CityRemark] = cityTop2.map {
            case (cityName, cityCount) => {
                val ratio: Double = cityCount.toDouble / totalCount
                CityRemark(cityName, ratio)
            }
        }
        // 3. 剩下的合并到其他
        //        var otherRadio = 1D
        //        cityRemarks.foreach(otherRadio -= _.cityRatio)
//        val otherRadio: Double = cityRemarks.foldLeft(1D)(_ - _.cityRatio)
        val otherRadio: Double = (1D /: cityRemarks)(_ - _.cityRatio)
        cityRemarks = cityRemarks :+ CityRemark("其他", otherRadio)
        // 4. 拼接成字符串
        cityRemarks.mkString(", ")
    }

    case class CityRemark(cityName: String, cityRatio: Double) {
        val formatter = new DecimalFormat("0.00%")

        override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
    }


}

