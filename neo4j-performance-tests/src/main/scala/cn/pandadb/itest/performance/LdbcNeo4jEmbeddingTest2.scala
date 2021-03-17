package cn.pandadb.itest.performance

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.function.Consumer

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import scala.collection.mutable.ArrayBuffer

object LdbcNeo4jEmbeddingTest2 {
  var db: GraphDatabaseService = null

  def main(args: Array[String]): Unit = {
//    val args = Array("testdata/db2", "testdata/args.txt")

    if(args.length < 1) {
      println("输入neo4j数据路径")
      return
    }
    val dbPath = args(0)
    println(s"数据路径： ${dbPath}")

    val cyphersArgs = ArrayBuffer[Array[String]]()

    if(args.length > 1) {
      val idPath: String = args(1)
      println(s"cypher参数文件路径：${idPath}")
      try {
        val br = new BufferedReader(new InputStreamReader(new FileInputStream(idPath)))
        br.lines().forEach(new Consumer[String] {
          override def accept(t: String): Unit = cyphersArgs.append(t.split(','))
        })

        br.close() //关闭IO
      } catch {
        case e: Exception =>
          System.out.println("文件操作失败")
          e.printStackTrace()
      }
    }
    else {
      println("需要参数文件路径")
      return
    }

    initDB(dbPath)

    runTest(cyphersArgs.toArray)

    db.shutdown()
  }

  def initDB(dbPath: String): Unit = {
    val dbFile = new File(dbPath)
    if (!dbFile.exists()) {
      throw new Exception(s"data not exist: $dbPath")
    }
    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).
      newGraphDatabase()
    println(s"已加载数据库：$dbPath")
  }

  def timing[T](f: => T): T = {
    val t1 = System.currentTimeMillis()
    val t = f
    val t2 = System.currentTimeMillis()
    println(s"time cost: ${t2 - t1} ms")
    t
  }

  def runCypher(cypher:String): Int = {
    val res = db.execute(cypher)
    var resCount = 0
    while (res.hasNext) {
      println(res.next())
      resCount += 1
    }
    resCount
  }


  def runTest(cyphersArgs: Array[Array[String]]): Unit = {

    assert(cypherTemlates.length == cyphersArgs.length)

    runCypher("match (n) return n limit 1")

    val timeUsed = scala.collection.mutable.ArrayBuffer[String]()

    for (i <- 0 until cypherTemlates.length) {
      println(s"---- cypher $i ----")
      val cypherTpl = cypherTemlates(i)
      val cypherArgs = cyphersArgs(i)
      println(s"args length: ${cypherArgs.length}")

      val t0 = System.currentTimeMillis()
      val resCount = ArrayBuffer[Int]()
      for (k <- 0 until cypherArgs.length) {
        val t1 = System.currentTimeMillis()
        val cypherStr = cypherTpl.format(cypherArgs(k))
        println(cypherStr)
        val resSize = runCypher(cypherStr)
        resCount.append(resSize)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis() - t0
      val avgUsed = allUsed / cypherArgs.length

      val tmp = s"cypher $i: argsLength:${cypherArgs.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms resultsLength: ${resCount.toList}"
      println(tmp)
      timeUsed.append(tmp)
    }

    println("==== 测试结束 ====")
    timeUsed.foreach(println(_))
  }


  val cypherTemlates = Array[String](
    """MATCH(n:person{firstName: "%s"})
      |   RETURN n""".stripMargin.replaceAll("\r\n"," "),  //1

    """MATCH (m:comment {id: "%s"})
      |RETURN
      |  m.creationDate AS messageCreationDate,
      |  m.content as content""".stripMargin.replaceAll("\r\n"," "), //2

    """MATCH (n:person {id:"%s"})-[r:knows]-(friend)
      |RETURN id(friend)""".stripMargin.replaceAll("\r\n"," "),     // 3

    """MATCH (n:person {id:"%s"})-[r:knows]-(friend)
      |RETURN
      |  friend.id AS personId,
      |  friend.firstName AS firstName,
      |  friend.lastName AS lastName,
      |  r.creationDate AS friendshipCreationDate""".stripMargin.replaceAll("\r\n"," "),  //4

    """MATCH (n:person {id:"%s"})-[:isLocatedIn]->(p:place)
      |RETURN
      |  n.firstName AS firstName,
      |  n.lastName AS lastName,
      |  n.birthday AS birthday,
      |  n.locationIP AS locationIP,
      |  n.browserUsed AS browserUsed,
      |  p.id AS cityId,
      |  n.gender AS gender,
      |  n.creationDate AS creationDate""".stripMargin.replaceAll("\r\n"," "),  //5

    """MATCH (m:message {id:"%s"})-[:hasCreator]->(p:person)
      |RETURN
      |  p.id AS personId,
      |  p.firstName AS firstName,
      |  p.lastName AS lastName""".stripMargin.replaceAll("\r\n"," "),  //6

    """MATCH (n:person {id:"%s"}) -[:knows]-> () -[:knows]-> (m:person)
      |RETURN id(m)""".stripMargin.replaceAll("\r\n"," "),    //7

    """MATCH (n:person {id:"%s"}) -[:knows]-> () -[:knows]-> (m:person)
      |RETURN m.firstName AS firstName,
      |       m.lastName AS lastName,
      |       m.birthday AS birthday,
      |       m.locationIP AS locationIP,
      |       m.browserUsed AS browserUsed""".stripMargin.replaceAll("\r\n"," "),  //8

    """MATCH (:person {id:"%s"})<-[:hasCreator]-(m)-[:replyOf]->(p:post)-[:hasCreator]->(c)
      |RETURN
      |  m.id AS messageId,
      |  m.creationDate AS messageCreationDate,
      |  p.id AS originalPostId,
      |  c.id AS originalPostAuthorId,
      |  c.firstName AS originalPostAuthorFirstName,
      |  c.lastName AS originalPostAuthorLastName""".stripMargin.replaceAll("\r\n"," "),  //9

    """MATCH (m:comment{id:"%s"})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)
      |RETURN
      |  f.id AS forumId,
      |  f.title AS forumTitle,
      |  mod.id AS moderatorId,
      |  mod.firstName AS moderatorFirstName,
      |  mod.lastName AS moderatorLastName""".stripMargin.replaceAll("\r\n"," "),  //10

    """MATCH (m:post{id:"%s"})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)
      |RETURN
      |  c.id AS commentId,
      |  c.content AS commentContent,
      |  c.creationDate AS commentCreationDate,
      |  p.id AS replyAuthorId,
      |  p.firstName AS replyAuthorFirstName,
      |  p.lastName AS replyAuthorLastName""".stripMargin.replaceAll("\r\n"," "),  //11

    """MATCH (m:post{id:"%s"})-[:hasCreator]->(a:person)-[r:knows]-(p)
      |RETURN
      |  m.id AS postId,
      |  m.language as postLanguage,
      |  p.id AS replyAuthorId,
      |  p.firstName AS replyAuthorFirstName,
      |  p.lastName AS replyAuthorLastName""".stripMargin.replaceAll("\r\n"," ") //12

  )


}
