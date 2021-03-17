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

object LdbcTestByStoreApiAllTest {
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


  def LDBC_short1(id:String): Iterable[Map[String, Any]] = {
    val results = ArrayBuffer[Map[String,Any]]()
    val nodes = graphFacade.nodes( NodeFilter(Seq("person"), Map("id"->LynxValue(id))))
    val edgeTypeId = relationStore.getRelationTypeId("isLocatedIn")
    val toNodeLabelId = nodeStore.getLabelId("place")
    nodes.foreach(startNode => {
      val outEdges = relationStore.findOutRelations(startNode.id.value.asInstanceOf[Long], Some(edgeTypeId))
      outEdges.foreach(relation => {
        val endNode = nodeStore.getNodeById(relation.to, Some(toNodeLabelId)).get
        results.append(Map[String,Any](
          "firstName" -> startNode.property("firstName"),
          "lastName" -> startNode.property("lastName"),
          "birthday" -> startNode.property("birthday"),
          "locationIP" -> startNode.property("locationIP"),
          "browserUsed" -> startNode.property("browserUsed"),
          "cityId" -> endNode.properties(nodeStore.getPropertyKeyId("id")),
          "gender" -> startNode.property("gender"),
          "creationDate" -> startNode.property("creationDate"),
        ))
      })
    })
    results
  }




  def LDBC_short2(id: String): Iterator[Map[String, Any]] ={
    val results = ArrayBuffer[Map[String,Any]]()

    val hasCreatorRelTypeId = relationStore.getRelationTypeId("hasCreator")
    val replyOfRelTypeId = relationStore.getRelationTypeId("replyOf")

    val postLabelId = nodeStore.getLabelId("post")
    val propKeyId_id = nodeStore.getPropertyKeyId("id")
    val propKeyId_creationDate = nodeStore.getPropertyKeyId("creationDate")
    val propKeyId_firstName = nodeStore.getPropertyKeyId("firstName")
    val propKeyId_lastName = nodeStore.getPropertyKeyId("lastName")

    val personNodes = graphFacade.nodes( NodeFilter(Seq("person"), Map("id"->LynxValue(id))))
    personNodes.foreach(p => {
      relationStore.findFromNodeIds(p.longId, hasCreatorRelTypeId).foreach( msgId => {
        relationStore.findToNodeIds(msgId, replyOfRelTypeId).foreach(postId => {
          if(nodeStore.hasLabel(postId, postLabelId)) {
          relationStore.findToNodeIds(postId, hasCreatorRelTypeId).foreach(creatorId => {
            val msgNode: StoredNodeWithProperty = nodeStore.getNodeById(msgId).get
            val postNode: StoredNodeWithProperty = nodeStore.getNodeById(postId).get
            val creatorNode: StoredNodeWithProperty = nodeStore.getNodeById(creatorId).get
            val map =  Map("messageId" -> msgNode.properties.get(propKeyId_id),
              "messageCreationDate"-> msgNode.properties.get(propKeyId_creationDate),
              "originalPostId"-> postNode.properties.get(propKeyId_id),
              "originalPostAuthorId"-> creatorNode.properties.get(propKeyId_id),
              "originalPostAuthorFirstName"->creatorNode.properties.get(propKeyId_firstName),
              "originalPostAuthorLastName"->creatorNode.properties.get(propKeyId_lastName)
            )
            results.append(map)
            if(results.length >= 10){
              return results.toIterator
            }
          })
        }}
        )
      })
    })
    results.toIterator
  }


  def LDBC_short3(id: String): Iterator[Map[String, Any]] ={
    val results = ArrayBuffer[Map[String,Any]]()

    val edgeTypeId_konws = relationStore.getRelationTypeId("knows")
    val propKeyId_id = nodeStore.getPropertyKeyId("id")
//    val propKeyId_creationDate = nodeStore.getPropertyKeyId("creationDate")
    val propKeyId_firstName = nodeStore.getPropertyKeyId("firstName")
    val propKeyId_lastName = nodeStore.getPropertyKeyId("lastName")

    val relPropKeyId_creationDate = relationStore.getPropertyKeyId("creationDate")

    val personNodes = graphFacade.nodes( NodeFilter(Seq("person"), Map("id"->LynxValue(id))))
    personNodes.foreach( p => {
      (relationStore.findInRelations(p.longId, Some(edgeTypeId_konws)).map(r => ( r, r.from)) ++
        relationStore.findOutRelations(p.longId, Some(edgeTypeId_konws)).map(r => ( r, r.to))).foreach(r => {
        val friendNode = nodeStore.getNodeById(r._2).get
        val relationWithProperty = relationStore.getRelationById(r._1.id).get
        val map = Map(
          "personId" -> friendNode.properties.get(propKeyId_id),
          "firstName" -> friendNode.properties.get(propKeyId_firstName),
          "lastName" -> friendNode.properties.get(propKeyId_lastName),
          "friendshipCreationDate" -> relationWithProperty.properties.get(relPropKeyId_creationDate)
        )
        results.append(map)
      })
    })

    results.toIterator
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

  def LDBC_short5(id: String): Iterator[Map[String, Any]] ={
    val results = ArrayBuffer[Map[String,Any]]()

    val hasCreatorRelTypeId = relationStore.getRelationTypeId("hasCreator")
    val toNodeLabelId = nodeStore.getLabelId("person")
    val propKeyId_id = nodeStore.getPropertyKeyId("id")
    //    val propKeyId_creationDate = nodeStore.getPropertyKeyId("creationDate")
    val propKeyId_firstName = nodeStore.getPropertyKeyId("firstName")
    val propKeyId_lastName = nodeStore.getPropertyKeyId("lastName")

    val nodes = graphFacade.nodes( NodeFilter(Seq("post"), Map("id"->LynxValue(id))))
    nodes.foreach(startNode => {
      val outEdges = relationStore.findOutRelations(startNode.id.value.asInstanceOf[Long], Some(hasCreatorRelTypeId))
      outEdges.foreach(relation => {
        val endNode = nodeStore.getNodeById(relation.to, Some(toNodeLabelId)).get
        results.append(Map[String,Any](
          "personId" -> endNode.properties.get(propKeyId_id),
          "firstName" -> endNode.properties.get(propKeyId_firstName),
          "lastName" -> endNode.properties.get(propKeyId_lastName)
        ))
      })
    })
    results.toIterator
  }


  def LDBC_short6(id: String): Iterator[Map[String, Any]] = {
    val results = ArrayBuffer[Map[String,Any]]()

    val edgeTypeId_replyOf = relationStore.getRelationTypeId("replyOf")
    val edgeTypeId_containerOf = relationStore.getRelationTypeId("containerOf")
    val edgeTypeId_hasModerator = relationStore.getRelationTypeId("hasModerator")

    val nodeLabelId_comment = nodeStore.getLabelId("comment")
    val nodeLabelId_post = nodeStore.getLabelId("post")
    val nodeLabelId_forum = nodeStore.getLabelId("forum")
    val nodeLabelId_person = nodeStore.getLabelId("person")

    val propKeyId_id = nodeStore.getPropertyKeyId("id")
    val propKeyId_title = nodeStore.getPropertyKeyId("title")
    val propKeyId_firstName = nodeStore.getPropertyKeyId("firstName")
    val propKeyId_lastName = nodeStore.getPropertyKeyId("lastName")

    val nodes = graphFacade.nodes( NodeFilter(Seq("comment"), Map("id"->LynxValue(id))))
    nodes.foreach(startNode => {
      val postIds = relationStore.findToNodeIds(startNode.longId, edgeTypeId_replyOf).filter(postId=>
        nodeStore.hasLabel(postId, nodeLabelId_post))
      postIds.foreach(postId => {
        val forums = relationStore.findFromNodeIds(postId,edgeTypeId_containerOf).filter(fId => {
          nodeStore.hasLabel(fId, nodeLabelId_forum)
        })
        forums.foreach(forumId => {
          val persons = relationStore.findToNodeIds(forumId, edgeTypeId_hasModerator).filter(personId => {
            nodeStore.hasLabel(personId, nodeLabelId_person)
          })
          persons.foreach(personId => {
            val forumNode: StoredNodeWithProperty = nodeStore.getNodeById(forumId, Some(nodeLabelId_forum)).get
            val personNode: StoredNodeWithProperty = nodeStore.getNodeById(personId, Some(nodeLabelId_person)).get
            results.append(Map[String,Any](
              "forumTitle"-> forumNode.properties.get(propKeyId_title),
              "forumId"->forumNode.properties.get(propKeyId_id),
              "moderatorId"-> personNode.properties.get(propKeyId_id),
              "moderatorFirstName"-> personNode.properties.get(propKeyId_firstName),
              "moderatorLastName"-> personNode.properties.get(propKeyId_lastName)
            ))
          })
        })
      })
    })
    results.toIterator
  }

  def LDBC_short7(id: String): Iterator[Map[String, Any]] ={
    val results = ArrayBuffer[Map[String,Any]]()

    val relTypeId_replyOf = relationStore.getRelationTypeId("replyOf")
    val relTypeId_hasCreator = relationStore.getRelationTypeId("hasCreator")
    val relTypeId_knows = relationStore.getRelationTypeId("knows")

    val nodeLabelId_person = nodeStore.getLabelId("person")
    val nodeLabelId_comment = nodeStore.getLabelId("comment")

    val propKeyId_id = nodeStore.getPropertyKeyId("id")
    val propKeyId_content = nodeStore.getPropertyKeyId("content")
    val propKeyId_creationDate = nodeStore.getPropertyKeyId("creationDate")
    val propKeyId_firstName = nodeStore.getPropertyKeyId("firstName")
    val propKeyId_lastName = nodeStore.getPropertyKeyId("lastName")

    val postNodes = graphFacade.nodes( NodeFilter(Seq("post"), Map("id"->LynxValue(id))))

    var repliers: Iterator[Long] = null
    postNodes.foreach(postNode => {
      val authors = relationStore.findToNodeIds(postNode.longId, relTypeId_hasCreator).filter(aId =>
        nodeStore.hasLabel(aId, nodeLabelId_person))

      if (authors.nonEmpty) {
        val authorId = authors.next()
        val authorNode = nodeStore.getNodeById(authorId, Some(nodeLabelId_person)).get

        val comments = relationStore.findFromNodeIds(postNode.longId, relTypeId_replyOf)
          .filter(cId => nodeStore.hasLabel(cId, nodeLabelId_comment))
        comments.foreach( commentId => {
          val repliers = relationStore.findToNodeIds(commentId, relTypeId_hasCreator)
            .filter(creatorId => nodeStore.hasLabel(creatorId, nodeLabelId_person))
          val commentNode = nodeStore.getNodeById(commentId, Some(nodeLabelId_comment)).get

          repliers.foreach( replierId => {
            if (relationStore.findOutRelationsBetween(authorId, replierId, Some(relTypeId_knows)).nonEmpty ||
              relationStore.findOutRelationsBetween(replierId, authorId, Some(relTypeId_knows)).nonEmpty) {
              val map = Map(
                "commentId"-> commentNode.properties.get(propKeyId_id),
                "commentContent" -> commentNode.properties.get(propKeyId_content),
                "commentCreationDate"-> commentNode.properties.get(propKeyId_creationDate),
                "replyAuthorId"-> authorNode.properties.get(propKeyId_id),
                "replyAuthorFirstName"-> authorNode.properties.get(propKeyId_firstName),
                "replyAuthorLastName"-> authorNode.properties.get(propKeyId_lastName)
              )
              results.append(map)
            }
          }

          )
        })
      }
    })

    results.toIterator
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
        val results = LDBC_short1(personIds(i)).toList.toList
        resCount.append(results.length)
        results.foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher1: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}")
    })

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
