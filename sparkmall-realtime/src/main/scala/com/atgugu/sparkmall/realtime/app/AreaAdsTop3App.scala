package com.atgugu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsTop3App {
    /**
      * 统计每个地区每天点击量的 top3
      *
      * @param areaCityAdsCount (2019-03-11:华南:深圳:1,2)
      */
    def statAreaAdsTop3(areaCityAdsCount: DStream[(String, Long)]) = {
        val dayAreaAdsCount: DStream[(String, Long)] = areaCityAdsCount.map {
            case (dayAreaCityAds, count) => { // (2019-03-11:华南:深圳:1, 2)
                val split: Array[String] = dayAreaCityAds.split(":")
                (s"${split(0)}:${split(1)}:${split(3)}", count) // (2019-03-11:华南:1,2)
            }
        }.reduceByKey(_ + _) // DSteam[(2019-03-11:华南:1, 200)]

        // 从 DSteam[(2019-03-11:华南:1, 2)] => map: DSteam[(2019-03-11, (华南, 1, 200)] => groupByKey: DSteam[(2019-03-11, Iterable[(华南, 1, 200)]]
        val areaAdsGroupByDay: DStream[(String, Iterable[(String, (String, Long))])] = dayAreaAdsCount.map {
            case (dayAreaAds, count) => {
                val split: Array[String] = dayAreaAds.split(":")
                (split(0), (split(1), (split(2), count)))
            }
        }.groupByKey
        // DSteam[(2019-03-11, Iterable[(华南, (1, 200))]]
        val resultDSteam: DStream[(String, Map[String, String])] = areaAdsGroupByDay.map {
            case (day, it) => {
                // Map[华南, Iterable[华南, (1, 200)]]
                val tem1: Map[String, Iterable[(String, (String, Long))]] = it.groupBy(_._1)
                // 调整结果把多Iterable 中冗余的 area 去掉 Map[(华南, it[(1, 200)]]
                val temp2: Map[String, Iterable[(String, Long)]] = tem1.map {
                    case (area, it) => { //(day, it[(String, (String, Long))])
                        (area, it.map(_._2)) // (day, it[(aids, count)]
                    }
                }

                // 对Map中的迭代器降序, 取前3, 转 json 字符串
                val temp3: Map[String, String] = temp2.map {
                    case (area, it) => {
                        val list: List[(String, Long)] = it.toList.sortWith(_._2 > _._2).take(3)
                        import org.json4s.JsonDSL._ // 加载的隐式转换  json4s  是面向 scala 的 json 转换
                        val adsCountJson: String = JsonMethods.compact(JsonMethods.render(list))
                        (area, adsCountJson)
                    }
                }

                (day, temp3)
            }
        }

        // 写入到redis 中


        resultDSteam.foreachRDD{
            rdd => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                val arr: Array[(String, Map[String, String])] = rdd.collect
                import scala.collection.JavaConversions._  // 导入隐式转换, 用于把 scala 的 map 隐式转换成 Java 的 map
                arr.foreach{
                    case (day, map) => {
                        jedisClient.hmset(s"area:ads:top3:$day", map)
                    }
                }
                jedisClient.close()
            }
        }
    }
}

/*
1. 数据来源:  使用上个需求的结果 (2019-03-11:华南:深圳:1,2)



2. 最终存储到 redis 的数据格式:
    使用 set 存储

    key  每天一个 key, 用当天的日期做后缀
         area:ads:top3:2019-03-22
    field               value(使用 json 字符串来存储)
    华南                {广告1: 1000, 广告2: 500}
    华北                {广告3: 1000, 广告1: 500}

 */