package two

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

object WordCount {
  def main(args: Array[String]) {
    // 设定执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 指定数据源地址，读取输入数据
    val text = env.readTextFile("D:\\input\\1.txt")
    // 对数据集指定转换操作逻辑
    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(_._1).sum(1)
    // 指定计算结果输出位置
    class MyMapFunction extends MapFunction[String, String] {
      override def map(t: String): String = {
        t.toUpperCase()
      }
    }

    println("输出结果")
    counts.writeAsText("")
    counts.writeUsingOutputFormat(new TextOutputFormat[(String, Int)](new Path("D:\\Output\\")))
    counts.print()

    // 指定名称并触发流式任务
    env.execute("Streaming WordCount")
  }


}
