package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategorySessionApp {

    def statCategoryTop10Session(spark: SparkSession, categoryTop10: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {
        // 1. 过滤掉 category 不在前 10 的日志
        // 1.1 得到 top10 的 categoryId
        val categoryIdTop10 = categoryTop10.map(_.categoryId)
        val categoryIdTop10BD = spark.sparkContext.broadcast(categoryIdTop10) // 广播变量
        // 1.2 过滤出来categorytop10的日志
        val filteredActionRDD = userVisitActionRDD.filter(info => categoryIdTop10BD.value.contains(info.click_category_id + ""))

        // 2. 转换结果为 RDD[(categoryId, sessionId), 1]  然后统计数量
        val categorySessionCountRDD = filteredActionRDD
            .map(userAction => ((userAction.click_category_id, userAction.session_id), 1))
            .reduceByKey(_ + _)

        // 3. 统计每个品类top10.  => RDD[categoryId, (sessionId, count)] => RDD[categoryId, Iterable[(sessionId, count)]]
        val categorySessionGrouped = categorySessionCountRDD.map {
            case ((cid, sid), count) => (cid, (sid, count))
        }.groupByKey

        // 4. 对每个 Iterable[(sessionId, count)]进行排序, 并取每个Iterable的前10
        // 5. 把数据封装到 CategorySession 中
        val sortedCategorySession = categorySessionGrouped.flatMap {
            case (cid, it) => {
                it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
                    item => CategorySession(taskId, cid.toString, item._1, item._2)
                }
            }
        }
        // 6. 写入到 mysql 数据库
        val categorySessionArr = sortedCategorySession.collect.map(item => Array(item.taskId, item.categoryId, item.sessionId, item.clickCount))
        JDBCUtil.executeUpdate("truncate category_top10_session_count", null)
        JDBCUtil.executeBatchUpdate("insert into category_top10_session_count values(?, ?, ?, ?)", categorySessionArr)
    }

}
/*

过滤出来只包含 top10 categoryId的 RDD[UserVisitAction].map =>
RDD[(categoryId, sessionId), 1].reduceByKey =>
RDD[(categoryId, sessionId), count].map =>
RDD[categoryId, (sessionId, count)].groupByKey  =>
RDD[categoryId, Iterator[(sessionId, count)]] => 排序, 取前10

 */