package com.atgugu.sparkmall.realtime.app

import java.util

import com.atgugu.sparkmall.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

object BlackListApp {
    // redis的一些相关参数
    val redisIp = "hadoop201"
    val redisPort = 6379
    val countKey = "user:day:adsclick"
    val blackListKey = "blacklist"

    // 过滤黑名单中的用户. 返回值是不包含黑名单的用户的广告点击记录的 DStream
    def checkUserFromBlackList(adsClickInfoDStream: DStream[AdsInfo], sc: SparkContext) = {
        adsClickInfoDStream.transform(rdd => {
            val jedis = RedisUtil.getJedisClient
            // 读出来黑名单
            val blackList: util.Set[String] = jedis.smembers(blackListKey)
            val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackList)
            jedis.close()
            rdd.filter(adsInfo => {
                !blackListBC.value.contains(adsInfo.userId)
            })
        })
    }


    // 把用户写入到黑名单中
    def checkUserToBlackList(adsInfoDStream: DStream[AdsInfo]) = {
        adsInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(adsInfoIt => {
                val jedis = RedisUtil.getJedisClient
                // 建立redis 连接
                adsInfoIt.foreach(adsInfo => {
                    //1. 在 redis 中计数. 使用 set Key: user:day:adsId   field: ...
                    val countField =
                        s"${adsInfo.userId}:${adsInfo.dayString}:${adsInfo.adsId}"
                    jedis.hincrBy(countKey, countField, 1) // 计数 + 1
                    // 2. 达到阈值后写入黑名单
                    if (jedis.hget(countKey, countField).toLong >= 10000) { // 如果点击某个广告的数量超过 100
                        jedis.sadd(blackListKey, adsInfo.userId) // 加入黑名单
                    }
                })
                jedis.close()
            })
        })
    }
}
