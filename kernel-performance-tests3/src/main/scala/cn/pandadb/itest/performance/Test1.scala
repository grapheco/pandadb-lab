package cn.pandadb.itest.performance

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.cache.CachedNodeStoreAPI
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI

object Test1 {
  def main(args: Array[String]): Unit = {
    val dbPath = "testdata/d1"
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

//    graphFacade.cypher("create(n:person:worker{name:'xx',arr1:[],arr2:[1,2,3],arr3:[1.1,3.5], arr4:['abc','dd','g'],arr5:[true]}) return n").show()
    graphFacade.cypher("create(n:person:worker{name:'xx'} )return n").show()
    val res = graphFacade.cypher("match(n:person{name:'xx'}) return n").records().next()
    println(res)
  }
}
