package cn.pandadb.itest.performance

import java.io.{File, FileInputStream, InputStreamReader}

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, PandaNode, RelationStoreSPI, StoredNodeWithProperty, StoredRelation}
import cn.pandadb.kernel.util.Profiler
import org.grapheco.lynx.{LynxNull, LynxValue, NodeFilter, RelationshipFilter}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.util.Random
import java.io.BufferedReader
import java.io.FileInputStream

import scala.collection.mutable.ArrayBuffer

object LdbcTestByStoreApi {
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

    var personId = Array("700000002540559", "708796094477119", "719791211845135", "728587303793103", "724189257648007", "700000003592071", "700000001856967", "717592187897287", "719791210631319", "719791211872543", "704398049851831", "700000001884183", "730786326912663", "726388282248071", "706597073343615", "715393163980831", "713194141415295", "724189256634711", "721990233113431", "713194140097367", "708796095517375", "702199023334903", "700000001229487", "717592186099143", "732985348893615", "713194139597231", "704398046691111", "717592186219303", "704398050067911", "710995116332047", "706597073323975", "702199023261503", "713194140404639")
    var postId = Array("787032086215979", "787032086215980", "787032086215981", "787032086215982", "787032086215983", "787032086215984", "787032086215985", "787032086215986", "787032086215987", "716663342038324", "646294597860670", "716663342038354", "787032086216038", "751847714127216", "857400830393722", "681478969949592", "751847714127266", "716663342038444", "892585202482614", "927769574571466", "822216458305000", "822216458305020", "927769574571526", "857400830393872", "927769574571546", "716663342038574", "857400830393912", "892585202482764", "857400830393942", "751847714127456")
    var commentId = Array("827766353710741", "827766353710742", "827766353710743", "827766353710744", "827766353710745", "827766353710746", "827766353710747", "827766353710748", "827766353710749", "827766353710750", "827766353710751", "827766353710752", "827766353710753", "827766353710754", "827766353710755", "827766353710756", "827766353710757", "827766353710758", "827766353710759", "827766353710760", "475922632822442", "475922632822443", "475922632822444", "475922632822445", "475922632822446", "475922632822447", "475922632822448", "475922632822449", "475922632822450", "475922632822451", "475922632822452", "475922632822453", "475922632822454")

    if(args.length > 1) {
      val idPath: String = args(1)
      println(s"id 文件路径：${idPath}")
      try {
        val br = new BufferedReader(new InputStreamReader(new FileInputStream(idPath)))
        var linestr = null //按行读取 将每次读取一行的结果赋值给linestr

        personId = br.readLine().split(',')
        postId = br.readLine().split(',')
        commentId = br.readLine().split(',')

        br.close() //关闭IO

      } catch {
        case e: Exception =>
          System.out.println("文件操作失败")
          e.printStackTrace()
      }
    }
    var testTimes:Int = 1
    if (args.length > 2) {
      testTimes = args(2).toInt
    }

    setup(dbPath)
    val begin = System.currentTimeMillis()
    println(s"已创建Facade | time: ${begin}")
    println("开始测试")
    LDBC(personId=personId, postId = postId, commentId = commentId, times = testTimes)
    val end = System.currentTimeMillis()
    val use = end - begin
    println(s"测试结束 | time(Millis): ${use}  ")
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


//
//  def LDBC_short1(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
//      RelationshipFilter(Seq("isLocatedIn"),Map()), NodeFilter(Seq("place"), Map()),SemanticDirection.OUTGOING
//    ).map{
//      p =>
//        Map(
//          "firstName" -> p.startNode.property("firstName"),
//          "lastName" -> p.startNode.property("lastName"),
//          "birthday" -> p.startNode.property("birthday"),
//          "locationIP" -> p.startNode.property("locationIP"),
//          "browserUsed" -> p.startNode.property("browserUsed"),
//          "cityId" -> p.endNode.property("id"),
//          "gender" -> p.startNode.property("gender"),
//          "creationDate" -> p.startNode.property("creationDate"),
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }

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

    val propKeyId_id = nodeStore.getPropertyKeyId("id")
    val propKeyId_creationDate = nodeStore.getPropertyKeyId("creationDate")
    val propKeyId_firstName = nodeStore.getPropertyKeyId("firstName")
    val propKeyId_lastName = nodeStore.getPropertyKeyId("lastName")

    val personNodes = graphFacade.nodes( NodeFilter(Seq("person"), Map("id"->LynxValue(id))))
    personNodes.foreach(p => {
      relationStore.findFromNodeIds(p.longId, hasCreatorRelTypeId).foreach( msgId => {
        relationStore.findToNodeIds(msgId, replyOfRelTypeId).foreach(postId => {
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
        })
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

    val nodes = graphFacade.nodes( NodeFilter(Seq("person"), Map("id"->LynxValue(id))))
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

//  def LDBC_short7(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
//      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("comment"), Map()),SemanticDirection.INCOMING),
//      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
//      (RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.BOTH),
//      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("post"), Map("id"->LynxValue(id))),SemanticDirection.INCOMING),
//    ).map{
//      p =>
//        Map(
//          "commentId"-> p.head.endNode.property("id"),
//          "commentContent"->p.head.endNode.property("content"),
//          "commentCreationDate"->p.head.endNode.property("creationDate"),
//          "replyAuthorId"->p(1).endNode.property("id"),
//          "replyAuthorFirstName"->p(1).endNode.property("firstName"),
//          "replyAuthorLastName"->p(1).endNode.property("lastName")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }

  def randomId(array: Array[String]): String = {
    array(Random.nextInt(array.length))
  }



  def LDBC(personId:Array[String], postId:Array[String], commentId:Array[String], times: Int): Unit ={

    println(s"personId: ${personId.toList}")
    println(s"postId: ${postId.toList}")
    println(s"commentId: ${commentId.toList}")
    println(s"times: $times")

    Profiler.timing({
      println("preheat")
      LDBC_short2(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-1.cypher")
      for (i <- 0 until times)
        LDBC_short1(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-2.cypher")
      for (i <- 0 until times)
        LDBC_short2(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-3.cypher")
      for (i <- 0 until times)
        LDBC_short3(randomId(personId)).foreach(println)
    })

    Profiler.timing({
      println("interactive-short-5.cypher")
      for (i <- 0 until times)
        LDBC_short5(randomId(postId)).foreach(println)
    })

//    Profiler.timing({
//      println("interactive-short-7.cypher")
//      for (i <- 0 until times)
//        LDBC_short7(randomId(postId)).foreach(println)
//    })

    Profiler.timing({
      println("interactive-short-4.cypher")
      for (i <- 0 until times)
        LDBC_short4(randomId(commentId)).foreach(println)
    })

    Profiler.timing({
      println("interactive-short-6.cypher")
      for (i <- 0 until times)
        LDBC_short6(randomId(commentId)).foreach(println)
    })

  }

}
