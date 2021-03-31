package cn.pandadb.itest.performance

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.{NodeLabelStore, NodeStore, NodeStoreAPI}
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import cn.pandadb.kernel.util.Profiler

import scala.collection.mutable.ArrayBuffer

object GetNodesByIds {
  var nodeStoreAPI: NodeStoreAPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  def main(args: Array[String]): Unit = {
    //    val args = Array("testdata/db2", "testdata/args.txt")
    if(args.length < 1) {
      println("需要数据路径")
      return
    }
    val dbPath = args(0)
    println(s"数据路径： ${dbPath}")

    val nodeIds = ArrayBuffer[Long]()

    if(args.length > 1) {
      val idPath: String = args(1)
      println(s"id文件路径：${idPath}")
      try {
        val br = new BufferedReader(new InputStreamReader(new FileInputStream(idPath)))
        br.lines().forEach(line => nodeIds.append(line.trim().toLong))
        br.close() //关闭IO
      } catch {
        case e: Exception =>
          System.out.println("文件操作失败")
          e.printStackTrace()
      }
    }
    else {
      println("需要id参数文件路径")
      return
    }

    var onlyReturnId = false
    if(args.length < 3 && args(3).toBoolean) {
      onlyReturnId = args(3).toBoolean
    }

    setup(dbPath)

    val begin = System.currentTimeMillis()
    println(s"已创建Facade | time: ${begin}")
    println("开始测试")
    runTest(nodeIds.toArray, onlyReturnId)
    val end = System.currentTimeMillis()
    val use = end - begin
    println(s"测试结束 | time(Millis): ${use}  ")

    graphFacade.close()

  }

  def setup(dbPath: String="./testdata"): Unit = {
    println(s"数据路径：${dbPath}")
    nodeStoreAPI = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStoreAPI,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }



  def runTest(ids: Array[Long], onlyReturnId: Boolean): Unit = {
    val f1 = classOf[NodeStoreAPI].getDeclaredField("nodeStore")
    f1.setAccessible(true)
    val nodeStore: NodeStore = f1.get(nodeStoreAPI).asInstanceOf[NodeStore]
    val f2 = classOf[NodeStoreAPI].getDeclaredField("nodeLabelStore")
    f2.setAccessible(true)
    val nodeLabelStore: NodeLabelStore = f2.get(nodeStoreAPI).asInstanceOf[NodeLabelStore]

    val res = ArrayBuffer[Any]()


    val t0 = System.currentTimeMillis()

    if (!onlyReturnId) {
      println("with deserialized")
      for (id<-ids) {
        val r = nodeStoreAPI.getNodeById(id)
        res.append(r)
      }
    }
    else {
      println("without deserialized")
      for (id<-ids) {
        val r = nodeStoreAPI.getNodeBytes(id)
        res.append(r)
      }
    }

    println("结果数：" + res.size)

    val allUsed = System.currentTimeMillis() - t0
    val avgUsed = allUsed*1000 / ids.length

    println("==== 测试结束 ====")
    println(s"all used: $allUsed 毫秒 ; avg used: $avgUsed 微秒")

  }

}
