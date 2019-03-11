import java.util

import redis.clients.jedis.Jedis

object RedisTest {
    def main(args: Array[String]): Unit = {
        /*val jedis = new Jedis("hadoop201", 6379)
//        println(jedis.ping())
        jedis.hincrBy("my1", "a", 1)
        val ss: util.Set[String] = jedis.keys("my1")
        println(jedis.hkeys("my1"))
        println(jedis.getSet("my1", "1"))*/
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        println(System.getProperty("HADOOP_USER_NAME"))
    }
}
