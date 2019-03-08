package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.udf.CityClickCountUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaClickTop3App {
    def statAreaClickTop3Product(spark: SparkSession) = {
        // 注册 UDAF 函数
        spark.udf.register("city_remark", new CityClickCountUDAF)
        spark.sql("use sparkmall")
        // 1. 查询出来所有的点击记录, 并与 city_info 表连接, 得到每个城市所在的地区.
        spark.sql(
            """
              |select
              |    c.*,
              |    v.click_product_id
              |from city_info c join user_visit_action v on c.city_id=v.city_id
              |where v.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")
        // 2. 按照地区商品 id 分组, 统计出每个商品的总点击次数
        spark.sql(
            """
              |select
              |    t1.area,
              |    t1.click_product_id,
              |    count(*) click_count,
              |    city_remark(t1.city_name) remark
              |from t1
              |group by t1.area, t1.click_product_id
            """.stripMargin).createOrReplaceTempView("t2")
        // 3. 每个地区内按照点击次数降序排列
        spark.sql(
            """
              |select
              |    *,
              |    rank() over(partition by t2.area sort by t2.click_count desc) rank
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")
        // 4. 只取前三名. 并把结果保存在数据库中
        val conf = ConfigurationUtil("config.properties")
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        spark.sql(
            """
              |select
              |    t3.area,
              |    p.product_name,
              |    t3.click_count,
              |    t3.remark,
              |    t3.rank
              |from t3 join product_info p on t3.click_product_id=p.product_id
              |where t3.rank <= 3
            """.stripMargin)
            .write.mode(SaveMode.Overwrite)
            .jdbc(conf.getString("jdbc.url"), "area_click_top10", props)
    }
}

/*
1. 查询出来所有的点击记录, 并与 city 变连接, 得到每个城市所在的地区
    select c.*
    from city_info c join user_visit_action v on c.cit_id=v.city_id
    where v.click_product_id > -1

    t1
2. 按照地区分组和商品 id, 统计出来商品的点击次数

    select
        t1.area,
        t1.click_product_id,
        count(*) click_count
    from t1
    group by t1.area, t1.click_product_id

    t2

3. 每个地区内按照点击次数进行排名
    select
        *,
        rank() over(partition by t2.area sort by t2.click_count desc) rank
    from t2

    t3
4. 每个地区取 3 名

    select
        *
    from t3
    where rank <= 3

    t4

5. 前3名的商品中每个城市的点击量百分比


6. 自定义聚合函数

 */
