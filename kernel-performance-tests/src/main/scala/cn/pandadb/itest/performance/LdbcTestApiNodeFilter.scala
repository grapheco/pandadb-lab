package cn.pandadb.itest.performance

import cn.pandadb.itest.performance.LdbcTest.{graphFacade, indexStore, nodeStore, relationStore, statistics}
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import cn.pandadb.kernel.util.Profiler

object LdbcTestApiNodeFilter {

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  def main(args: Array[String]): Unit = {

    if(args.length < 1) {
      println("需要数据路径")
      return
    }
    val dbPath = args(0)
    println(s"数据路径： ${dbPath}")
    val label: String = args(1)
    val propKey: String = args(2)
    val propValue: Any = args(3)

    println(s"label: ${label} propertyKey: ${propKey} propertyValue: ${propValue}")
    newGraphFacade(dbPath)
    Profiler.timing({
      filterNodes(label, propKey, propValue)
    })

    graphFacade.close()
  }

  def filterNodes(label: String, propertyKey:String, propertyValue: Any): Unit = {
    val iter = graphFacade.getNodesByLabels(Seq(label), exact = false)
    val keyId = nodeStore.getPropertyKeyId(propertyKey)
    println(s"propertyKeyId: $keyId")
    var allNodeCount = 0
    var fits = 0
    while (iter.hasNext) {
      val node = iter.next()
      allNodeCount += 1
      if (node.properties.contains(keyId) && node.properties(keyId).equals(propertyValue))
        fits += 1
    }
    println(s"allNodeCount: ${allNodeCount}")
    println(s"fits: ${fits}")
  }

  def newGraphFacade(dbPath: String="./testdata"): GraphFacade = {
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
    graphFacade
  }

}
