package com.atgugu.sparkmall.realtime.app

import com.atgugu.sparkmall.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsPerDay {
    def statAreaCityAdsPerDay(filteredAdsInfoDStream: DStream[AdsInfo], sc: SparkContext) = {
        // 1. 统计数据

        sc.setCheckpointDir("./checkpoint")
        val resultDSteam: DStream[(String, Long)] = filteredAdsInfoDStream.map(adsInfo => {
            (s"${adsInfo.dayString}:${adsInfo.area}:${adsInfo.city}:${adsInfo.adsId}", 1L)
        }).reduceByKey(_ + _).updateStateByKey((seq: Seq[Long], opt:Option[Long]) => {
            Some(seq.sum + opt.getOrElse(0L))
        })

        // 2. 写入到 redis
        resultDSteam.foreachRDD(rdd => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val totalCountArr: Array[(String, Long)] = rdd.collect
            totalCountArr.foreach {
                case (field, count) => jedisClient.hset("day:area:city:adsCount", field, count.toString)
            }
            jedisClient.close()
        })
        resultDSteam
    }
}

/*
1. 统计数据
    rdd[AdsInfo] => rdd[date:area:city:ads, 1] => reduceByKey(_+_)

    => updaeStateByKey(...)

    设置 checkpoint

2. 写入到 redis
*/