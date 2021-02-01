package cn.pandadb.lab

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import java.text.SimpleDateFormat
import java.util.Date

object GraphNodeIndexCreator {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("参数不足")
      return
    }
    val dbPath: String = args(0)
    val label: String = args(1)
    val props: Set[String] = Set(args(2))
    println(s"数据路径：${dbPath} | 建立索引的标签：${label}, 属性：${props}")

    val graphFacade = newGraphFacade(dbPath)
    val begin = System.currentTimeMillis()
    println("begin: " + formatTime(begin))

    graphFacade.createIndexOnNode(label, props)

    val end = System.currentTimeMillis()
    println("end: " + formatTime(end))
    println(s"used time(ms): ${end-begin}")
    println(s"hasIndex(): ")
    println(graphFacade.hasIndex(Set(label), props))
    graphFacade.close()
  }



   def formatTime(millis: Long = System.currentTimeMillis()): String = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      df.format(new Date(millis))

    }


  def newGraphFacade(dbPath: String="./testdata"): GraphFacade = {
    println(s"数据路径：${dbPath}")
    val nodeStore = new NodeStoreAPI(dbPath)
    val relationStore = new RelationStoreAPI(dbPath)
    val indexStore = new IndexStoreAPI(dbPath)
    val statistics = new Statistics(dbPath)

    val graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
    graphFacade
  }

}
