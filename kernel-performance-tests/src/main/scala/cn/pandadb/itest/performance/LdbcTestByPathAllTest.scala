package cn.pandadb.itest.performance

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.grapheco.lynx.{LynxNull, LynxValue, NodeFilter, RelationshipFilter}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object LdbcTestByPathAllTest {
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


  def LDBC_short1(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
      RelationshipFilter(Seq("isLocatedIn"),Map()), NodeFilter(Seq("place"), Map()),SemanticDirection.OUTGOING
    ).map{
      p =>
        Map(
          "firstName" -> p.startNode.property("firstName"),
          "lastName" -> p.startNode.property("lastName"),
          "birthday" -> p.startNode.property("birthday"),
          "locationIP" -> p.startNode.property("locationIP"),
          "browserUsed" -> p.startNode.property("browserUsed"),
          "cityId" -> p.endNode.property("id"),
          "gender" -> p.startNode.property("gender"),
          "creationDate" -> p.startNode.property("creationDate"),
        ).mapValues(_.getOrElse(LynxNull))
    }
  }


  def LDBC_short2(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.INCOMING),
      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("post"), Map()),SemanticDirection.OUTGOING),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.OUTGOING),
    ).take(10).map{
      p =>
        Map("messageId" -> p.head.endNode.property("id"),
          "messageCreationDate"-> p.head.endNode.property("creationDate"),
          "originalPostId"->p(1).endNode.property("id"),
          "originalPostAuthorId"->p.last.endNode.property("id"),
          "originalPostAuthorFirstName"->p.last.endNode.property("firstName"),
          "originalPostAuthorLastName"->p.last.endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short3(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
      RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.BOTH
    ).map{
      p =>
        Map(
          "personId" -> p.endNode.property("id"),
          "firstName" -> p.endNode.property("firstName"),
          "lastName" -> p.endNode.property("lastName"),
          "friendshipCreationDate" -> p.storedRelation.property("creationDate")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short4(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.nodes(
      NodeFilter(Seq("comment"),Map("id"->LynxValue(id)))
    ).map{
      n =>
        Map(
          "createDate" -> n.property("creationDate"),
          "content" -> n.property("content")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short5(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
      RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING
    ).map{
      p =>
        Map(
          "personId" -> p.endNode.property("id"),
          "firstName" -> p.endNode.property("firstName"),
          "lastName" -> p.endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short6(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("comment"), Map("id"->LynxValue(id))),
      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("post"), Map()),SemanticDirection.OUTGOING),
      (RelationshipFilter(Seq("containerOf"),Map()), NodeFilter(Seq("forum"), Map()),SemanticDirection.INCOMING),
      (RelationshipFilter(Seq("hasModerator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
    ).map{
      p =>
        Map(
          "forumTitle"-> p.last.startNode.property("title"),
          "forumId"->p.last.startNode.property("id"),
          "moderatorId"->p.last.endNode.property("id"),
          "moderatorFirstName"->p.last.endNode.property("firstName"),
          "moderatorLastName"->p.last.endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short7(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("comment"), Map()),SemanticDirection.INCOMING),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
      (RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.BOTH),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("post"), Map("id"->LynxValue(id))),SemanticDirection.INCOMING),
    ).map{
      p =>
        Map(
          "commentId"-> p.head.endNode.property("id"),
          "commentContent"->p.head.endNode.property("content"),
          "commentCreationDate"->p.head.endNode.property("creationDate"),
          "replyAuthorId"->p(1).endNode.property("id"),
          "replyAuthorFirstName"->p(1).endNode.property("firstName"),
          "replyAuthorLastName"->p(1).endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
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

}
