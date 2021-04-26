package cn.pandadb.itest.performance

import cn.pandadb.kernel.kv.{ByteUtils, GraphFacade, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.kv.KeyConverter.{LabelId, NodeId}
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NodeLabelNameStore, PropertyNameStore, Statistics}
import cn.pandadb.kernel.kv.node.{NodeStore, NodeStoreAPI}
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, PandaNode, RelationStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.apache.commons.io.output.WriterOutputStream
import org.grapheco.lynx.{LynxValue, NodeFilter}

object ScanNodesTest {
  var nodeStoreKVDB: KeyValueDB = _
  var nodeLabelName:NodeLabelNameStore = _
  var propertyName: PropertyNameStore = _

  def main(args: Array[String]): Unit = {
    println("参数：<数据路径> <标签名称> <测试目标（默认1）：1-只返回ID;2-返回StoredNodeWithProperty;3-返回NodeId和未解析的属性>")
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
      println("需要指定测试目标")
      return
    }
    val testTarget = args(2)
    println(s"测试目标： ${testTarget}")

    setup(dbPath)
    val begin = System.currentTimeMillis()
    println(s"已创建Facade | time: ${begin}")
    println("开始测试")

    val res = scanNodes(labelName, testTarget)
    println("节点数：" + res.length)

    val end = System.currentTimeMillis()
    val use = end - begin
    println(s"测试结束 | time(Millis): ${use}  ")

    close()
  }

  def close(): Unit = {
    nodeStoreKVDB.close()
    nodeLabelName.close()
    propertyName.close()
  }


  def setup(dbPath: String="./testdata"): Unit = {
    println(s"数据路径：${dbPath}")
    nodeStoreKVDB = RocksDBStorage.getDB(s"${dbPath}/nodes", rocksdbConfigPath = "default")
    val metaDB: KeyValueDB = RocksDBStorage.getDB(s"${dbPath}/nodeMeta", rocksdbConfigPath = "default")
    nodeLabelName = new NodeLabelNameStore(metaDB)
    propertyName = new PropertyNameStore(metaDB)
  }

  def scanNodes(labelName: String, testTarget: String): Iterator[Any] = {
    var labelId: Int = -1
    if(labelName!="null"){
      labelId = nodeLabelName.id(labelName)
    }

    testTarget match {
      case "1" => getNodesIds(labelId)
      case "2" => getNodes(labelId)
      case "3" => getNodesWithoutDeserialize(labelId).map(_._2)
    }
  }


  def getNodesIds(labelId: LabelId = -1): Iterator[NodeId] = {
    println(s"getNodesIds(labelId: LabelId = $labelId)")
    val iter = nodeStoreKVDB.newIterator()
    if(labelId != -1) {
      val prefix = KeyConverter.toNodeKey(labelId)
      iter.seek(prefix)
      new Iterator[NodeId] (){
        override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
        override def next(): NodeId = {
          val id = ByteUtils.getLong(iter.key(), 4)
          iter.next()
          id
        }
      }
    }
    else {
      iter.seekToFirst()
      new Iterator[NodeId] (){
        override def hasNext: Boolean = iter.isValid
        override def next(): NodeId = {
          val id = ByteUtils.getLong(iter.key(), 4)
          iter.next()
          id
        }
      }
    }
  }

  def getNodes(labelId: LabelId = -1): Iterator[StoredNodeWithProperty] = {
    println(s"getNodes(labelId: LabelId = $labelId)")
    val iter = nodeStoreKVDB.newIterator()
    if(labelId != -1) {
      val prefix = KeyConverter.toNodeKey(labelId)
      iter.seek(prefix)
      new Iterator[StoredNodeWithProperty] (){
        override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
        override def next(): StoredNodeWithProperty = {
          val node = NodeSerializer.deserializeNodeValue(iter.value())
          iter.next()
          node
        }
      }
    }
    else {
      iter.seekToFirst()
      new Iterator[StoredNodeWithProperty] (){
        override def hasNext: Boolean = iter.isValid
        override def next(): StoredNodeWithProperty = {
          val node = NodeSerializer.deserializeNodeValue(iter.value())
          iter.next()
          node
        }
      }
    }

  }

  def getNodesWithoutDeserialize(labelId: LabelId = -1): Iterator[(NodeId,Byte)] = {
    println(s"getNodesWithoutDeserialize(labelId: LabelId = $labelId)")
    val iter = nodeStoreKVDB.newIterator()
    if(labelId != -1) {
      val prefix = KeyConverter.toNodeKey(labelId)
      iter.seek(prefix)
      new Iterator[(NodeId,Byte)] (){
        override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)
        override def next(): (NodeId,Byte) = {
          val id = ByteUtils.getLong(iter.key(), 4)
          val bytes = iter.value()
          iter.next()
          (id, bytes(bytes.length-2))
        }
      }
    }
    else {
      iter.seekToFirst()
      new Iterator[(NodeId,Byte)] (){
        override def hasNext: Boolean = iter.isValid
        override def next(): (NodeId,Byte) = {
          val id = ByteUtils.getLong(iter.key(), 4)
          val bytes = iter.value()
          iter.next()
          (id, bytes(bytes.length-2))
        }
      }
    }


  }
}
