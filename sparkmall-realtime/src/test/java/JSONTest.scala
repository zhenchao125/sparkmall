import com.alibaba.fastjson.JSON
import org.json4s.jackson.JsonMethods

object JSONTest {
    def main(args: Array[String]): Unit = {

        val list = List(("a", 1), ("b", 2))
        import org.json4s.JsonDSL._  // 加载的隐式转换
        println(JsonMethods.compact(JsonMethods.render(list)))

    }
}
