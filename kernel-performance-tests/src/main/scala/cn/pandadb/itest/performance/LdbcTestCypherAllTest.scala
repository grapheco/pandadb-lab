package cn.pandadb.itest.performance

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.grapheco.lynx.{LynxNull, LynxValue, NodeFilter}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object LdbcTestCypherAllTest {
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

    var personIds = Array("700000002540559", "708796094477119")
    var postIds = Array("787032086215979", "787032086215980")
    var commentIds = Array("827766353710741", "827766353710742")

    if(args.length > 1) {
      val idPath: String = args(1)
      println(s"id 文件路径：${idPath}")
      try {
        val br = new BufferedReader(new InputStreamReader(new FileInputStream(idPath)))
        var linestr = null //按行读取 将每次读取一行的结果赋值给linestr

        personIds = br.readLine().split(',')
        postIds = br.readLine().split(',')
        commentIds = br.readLine().split(',')

        br.close() //关闭IO

      } catch {
        case e: Exception =>
          System.out.println("文件操作失败")
          e.printStackTrace()
      }
    }

    setup(dbPath)
    val begin = System.currentTimeMillis()
    println(s"已创建Facade | time: ${begin}")
    println("开始测试")
    LDBC(personIds=personIds, postIds = postIds, commentIds = commentIds)
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




  def randomId(array: Array[String]): String = {
    array(Random.nextInt(array.length))
  }


  def LDBC(personIds:Array[String], postIds:Array[String], commentIds:Array[String]): Unit ={
    println(s"personId: ${personIds.toList}")
    println(s"postId: ${postIds.toList}")
    println(s"commentId: ${commentIds.toList}")

    val timeUsed = scala.collection.mutable.ArrayBuffer[String]()

//    Profiler.timing({
//      println("preheat")
//      LDBC_short2(randomId(personIds)).foreach(println)
//    })

    Profiler.timing({
      println("interactive-short-1.cypher")
      println(s"persons length: ${personIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until personIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short1(personIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher1: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })
    println(timeUsed.toList)

    Profiler.timing({
      println("interactive-short-2.cypher")
      println(s"persons length: ${personIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until personIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short2(personIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher2: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })
    println(timeUsed.toList)

    Profiler.timing({
      println("interactive-short-3.cypher")
      println(s"persons length: ${personIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until personIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short3(personIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher3: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })
    println(timeUsed.toList)

    Profiler.timing({
      println("interactive-short-4.cypher")
      println(s"comments length: ${commentIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until commentIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short4(commentIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/commentIds.length
      timeUsed.append( s"cypher4: comments:${commentIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })
    println(timeUsed.toList)

    Profiler.timing({
      println("interactive-short-5.cypher")
      println(s"posts length: ${postIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until postIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short5(postIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/postIds.length
      timeUsed.append( s"cypher5: posts:${postIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })

    println(timeUsed.toList)



    Profiler.timing({
      println("interactive-short-6.cypher")
      println(s"comments length: ${commentIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until commentIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short6(commentIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/commentIds.length
      timeUsed.append( s"cypher6: comments:${commentIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })

    println(timeUsed.toList)

    return

    Profiler.timing({
      println("interactive-short-7.cypher")
      println(s"posts length: ${postIds.length}")
      val t0 =System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (i <- 0 until postIds.length) {
        val t1 = System.currentTimeMillis()
        val results = LDBC_short7(postIds(i)).toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/postIds.length
      timeUsed.append( s"cypher7: posts:${postIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })


    println(timeUsed.toList)

  }

  def runCypher(cypher:String): Iterator[Any] = {
    val res = graphFacade.cypher(cypher)
//    val results = ArrayBuffer[Any]()
    val records = res.records()
    records
//    while (records.hasNext) {
//      results.append(records.next())
//    }
//    results
  }

  def LDBC_short1(personId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (n:person {id:"$personId"})-[:isLocatedIn]->(p:place)
                      RETURN  n.firstName AS firstName,  n.lastName AS lastName,  n.birthday AS birthday,
                        n.locationIP AS locationIP,  n.browserUsed AS browserUsed,  p.id AS cityId,
                        n.gender AS gender,  n.creationDate AS creationDate"""

    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short2(personId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (:person {id:"$personId"})<-[:hasCreator]-(m)-[:replyOf]->(p:post)
                      -[:hasCreator]->(c)
                      RETURN  m.id AS messageId,  m.creationDate AS messageCreationDate,
                        p.id AS originalPostId,  c.id AS originalPostAuthorId,
                        c.firstName AS originalPostAuthorFirstName,  c.lastName AS originalPostAuthorLastName
                      LIMIT 10"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short3(personId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (n:person {id:"$personId"})-[r:knows]-(friend)
                      RETURN
                        friend.id AS personId,  friend.firstName AS firstName,  friend.lastName AS lastName,
                        r.creationDate AS friendshipCreationDate"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short4(commentId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (m:comment {id:"$commentId"})
                    RETURN  m.creationDate AS messageCreationDate,  m.content as content"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short5(postId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (m:post {id:"$postId"})-[:hasCreator]->(p:person)
                    RETURN  p.id AS personId,  p.firstName AS firstName,  p.lastName AS lastName"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short6(commentId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (m:comment{id:"$commentId"})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)
                    RETURN
                      f.id AS forumId,  f.title AS forumTitle,  mod.id AS moderatorId,  mod.firstName AS moderatorFirstName,
                      mod.lastName AS moderatorLastName"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short7(postId: String): Iterator[Any] = {
    val cypherStr = s"""MATCH (m:post{id:"$postId"})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)
      MATCH (m)-[:hasCreator]->(a:person)-[r:knows]-(p)
      RETURN
        c.id AS commentId,  c.content AS commentContent,  c.creationDate AS commentCreationDate,
        p.id AS replyAuthorId,  p.firstName AS replyAuthorFirstName,  p.lastName AS replyAuthorLastName"""
    println(cypherStr)
    runCypher(cypherStr)
  }
}
