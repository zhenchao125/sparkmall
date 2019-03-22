package com.atguigu.sparkmall.offline

import java.text.DecimalFormat

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.bean.Condition

object Test {
    def main(args: Array[String]): Unit = {
        val conditions: Condition = readConditions
        println(isNotEmpty(conditions.startDate))
        println(isEmpty(conditions.startDate))
    }

    /**
      * 读取过滤条件
      *
      * @return
      */
    def readConditions: Condition = {
        // 读取配置文件
        val config = ConfigurationUtil("conditions.properties")
        // 读取到其中的 JSON 字符串
        val conditionString = config.getString("condition.params.json")
        // 解析成 Condition 对象
        JSON.parseObject(conditionString, classOf[Condition])
    }
}
