package cn.pandadb.itest.performance

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import org.neo4j.driver.{Driver, GraphDatabase, Session}

import scala.collection.mutable.ArrayBuffer

object Neo4jTest {
  var driver: Driver = null
  var session: Session = null
  def main(args: Array[String]): Unit = {


    var personIds = Array("728587305677895","713194141640247","708796096049431","704398047793663","730786327096127","708796093788047","721990233553719",
      "704398047653247","719791212536327","700000000677607")
    var postIds = Array("892585202482764","787032086217147","751847714130924","646294597865129","857400830398294","646294597865279","646294597865280",
      "787032086220165","787032086220166","857400830395884")
    var commentIds = Array("827766353710753","616660121177939","581475749089386","687028865356107","581475749089801","440738260734629","440738260734757",
      "440738260734843","440738260734844","440738260734845")

//    if(args.length > 1) {
//      val idPath: String = args(1)
//      println(s"id 文件路径：${idPath}")
//      try {
//        val br = new BufferedReader(new InputStreamReader(new FileInputStream(idPath)))
//        var linestr = null //按行读取 将每次读取一行的结果赋值给linestr
//
//        personIds = br.readLine().split(',')
//        postIds = br.readLine().split(',')
//        commentIds = br.readLine().split(',')
//
//        br.close() //关闭IO
//
//      } catch {
//        case e: Exception =>
//          System.out.println("文件操作失败")
//          e.printStackTrace()
//      }
//    }

    initDB()

    runTest(personIds, postIds, commentIds)

    session.close()
  }

  def initDB(): Unit = {
    driver = GraphDatabase.driver("bolt://10.0.82.239:7687")
    session = driver.session()
  }

  def timing[T](f: => T): T = {
    val t1 = System.currentTimeMillis()
    val t = f
    val t2 = System.currentTimeMillis()
    println(s"time cost: ${t2 - t1} ms")
    t
  }

  def runCypher(cypher:String): ArrayBuffer[Any] = {
    val res = session.run(cypher)
    val results = ArrayBuffer[Any]()
    while (res.hasNext) {
      results.append(res.next())
    }
    results
  }


  def runTest(personIds:Array[String], postIds:Array[String], commentIds:Array[String]): Unit = {
    println(s"personId: ${personIds.toList}")
    println(s"postId: ${postIds.toList}")
    println(s"commentId: ${commentIds.toList}")

    println(s"personId: ${personIds.toList}")
    println(s"postId: ${postIds.toList}")
    println(s"commentId: ${commentIds.toList}")

    val timeUsed = scala.collection.mutable.ArrayBuffer[String]()


    timing({
      println("interactive-short-1.cypher")
      println(s"persons length: ${personIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until personIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short1(personIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher1: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    timing({
      println("interactive-short-2.cypher")
      println(s"persons length: ${personIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until personIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short2(personIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher2: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    timing({
      println("interactive-short-3.cypher")
      println(s"persons length: ${personIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until personIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short3(personIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/personIds.length
      timeUsed.append( s"cypher3: persons:${personIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    timing({
      println("interactive-short-5.cypher")
      println(s"posts length: ${postIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until postIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short5(postIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/postIds.length
      timeUsed.append( s"cypher5: posts:${postIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    timing({
      println("interactive-short-7.cypher")
      println(s"posts length: ${postIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until postIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short7(postIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/postIds.length
      timeUsed.append( s"cypher7: posts:${postIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    timing({
      println("interactive-short-4.cypher")
      println(s"comments length: ${commentIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until commentIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short4(commentIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/commentIds.length
      timeUsed.append( s"cypher4: comments:${commentIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    timing({
      println("interactive-short-6.cypher")
      println(s"comments length: ${commentIds.length}")
      val t0 =System.currentTimeMillis()
      for (i <- 0 until commentIds.length) {
        val t1 = System.currentTimeMillis()
        LDBC_short6(commentIds(i)).foreach(println)
        val used = System.currentTimeMillis() - t1
        println(s"used(ms): ${used}")
      }
      val allUsed = System.currentTimeMillis()-t0
      val avgUsed = allUsed/commentIds.length
      timeUsed.append( s"cypher6: comments:${commentIds.length} allUsed:${allUsed}ms avgUsed:${avgUsed}ms")
    })

    println(timeUsed.toList)
  }

  def LDBC_short1(personId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (n:person {id:"$personId"})-[:isLocatedIn]->(p:place)
                      RETURN  n.firstName AS firstName,  n.lastName AS lastName,  n.birthday AS birthday,
                        n.locationIP AS locationIP,  n.browserUsed AS browserUsed,  p.id AS cityId,
                        n.gender AS gender,  n.creationDate AS creationDate"""

    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short2(personId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (:person {id:"$personId"})<-[:hasCreator]-(m)-[:replyOf]->(p:post)
                      -[:hasCreator]->(c)
                      RETURN  m.id AS messageId,  m.creationDate AS messageCreationDate,
                        p.id AS originalPostId,  c.id AS originalPostAuthorId,
                        c.firstName AS originalPostAuthorFirstName,  c.lastName AS originalPostAuthorLastName
                      LIMIT 10"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short3(personId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (n:person {id:"$personId"})-[r:knows]-(friend)
                      RETURN
                        friend.id AS personId,  friend.firstName AS firstName,  friend.lastName AS lastName,
                        r.creationDate AS friendshipCreationDate"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short4(commentId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (m:comment {id:$commentId})
                    RETURN  m.creationDate AS messageCreationDate,  m.content as content"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short5(postId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (m:post {id:$postId})-[:hasCreator]->(p:person)
                    RETURN  p.id AS personId,  p.firstName AS firstName,  p.lastName AS lastName"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short6(commentId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (m:comment{id:$commentId})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)
                    RETURN
                      f.id AS forumId,  f.title AS forumTitle,  mod.id AS moderatorId,  mod.firstName AS moderatorFirstName,
                      mod.lastName AS moderatorLastName"""
    println(cypherStr)
    runCypher(cypherStr)
  }

  def LDBC_short7(postId: String): Iterable[Any] = {
    val cypherStr = s"""MATCH (m:post{id:$postId})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)
      MATCH (m)-[:hasCreator]->(a:person)-[r:knows]-(p)
      RETURN
        c.id AS commentId,  c.content AS commentContent,  c.creationDate AS commentCreationDate,
        p.id AS replyAuthorId,  p.firstName AS replyAuthorFirstName,  p.lastName AS replyAuthorLastName"""
    println(cypherStr)
    runCypher(cypherStr)
  }



}
