package cn.pandadb.itest.performance

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

//import cn.pandadb.itest.performance.LdbcTestByStoreApi.LDBC
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, PandaNode, RelationStoreSPI}
import org.grapheco.lynx.{LynxValue, NodeFilter}

object NodesFilterTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  def main(args: Array[String]): Unit = {
    println("参数：<数据路径> <标签名称> <属性名称> <属性值> <属性值类型 string,int,float,boolean>")
    if(args.length < 1) {
      println("需要数据路径")
      return
    }
    val dbPath = args(0)
    println(s"数据路径： ${dbPath}")

    if(args.length < 2) {
      println("需要指定节点标签")
      return
    }
    val labelName = args(1)
    println(s"节点标签： ${labelName}")


    if(args.length < 3) {
      println("需要指定属性名称")
      return
    }
    val propKey = args(2)
    println(s"属性名称： ${propKey}")

    if(args.length < 4) {
      println("需要指定属性值")
      return
    }
    val propValue = args(3)
    println(s"属性值： ${propValue}")

    var propValueType = "string"
    var propValue2: Any = null
    if(args.length >= 4) {
      args(4) match {
        case "string" => propValue2 = propValue
        case "int" => propValue2 = propValue.asInstanceOf[Int]
        case "float" => propValue2 = propValue.asInstanceOf[Float]
        case "boolean" => propValue2 = propValue.asInstanceOf[Boolean]
        case _ => propValue2 = propValue
      }
    }

    setup(dbPath)
    val begin = System.currentTimeMillis()
    println(s"已创建Facade | time: ${begin}")
    println("开始测试")

    val res = filterNodes(labelName, propKey, propValue2)
    println("节点数：" + res.length)

    val end = System.currentTimeMillis()
    val use = end - begin
    println(s"测试结束 | time(Millis): ${use}  ")

    graphFacade.close()
  }



  def setup(dbPath: String="./testdata"): Unit = {
    println(s"数据路径：${dbPath}")
    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  def filterNodes(labelName: String, propKey: String, propValue: Any): Iterator[PandaNode] = {
    if(labelName=="null"){
      graphFacade.nodes(NodeFilter(Seq(), Map(propKey->LynxValue(propValue))))
    }
    else {
      graphFacade.nodes(NodeFilter(Seq(labelName), Map(propKey->LynxValue(propValue))))
    }
  }
}
