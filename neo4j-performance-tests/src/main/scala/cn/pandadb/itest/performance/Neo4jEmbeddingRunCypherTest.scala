package cn.pandadb.itest.performance

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.file.Files

import cn.pandadb.itest.performance.LdbcNeo4jEmbeddingTest.runTest
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

object Neo4jEmbeddingRunCypherTest {
  var db: GraphDatabaseService = null

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      println("输入neo4j数据路径")
      return
    }
    val dbPath = args(0)
    println(s"数据路径： ${dbPath}")

    if(args.length < 2) {
      println("输入执行的cypher文件路径")
      return
    }
    val file = new File(args(1))
    if(!file.exists()){
      println("file not exist")
      return
    }

    var cypher = ""

    try {
      val br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
      cypher = br.lines().toArray.reduce(_ + " " + _).asInstanceOf[String]
      br.close() //关闭IO

    } catch {
      case e: Exception =>
        System.out.println("文件操作失败")
        e.printStackTrace()
        return
    }

    if(cypher.trim.isEmpty) {
      println("cypher is empty")
      return
    }
    println(s"执行cypher: $cypher")

    initDB(dbPath)

    val begin = System.currentTimeMillis()
    println(s"已创建Facade | time: ${begin}")
    println("开始测试")

    val res = runCypher(cypher)
    println("结果条数：" + res)

    val end = System.currentTimeMillis()
    val use = end - begin
    println(s"测试结束 | time(Millis): ${use}  ")

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
    println("runCypher")
    val res = db.execute(cypher)
    println("run")
    var resCount = 0
    while (res.hasNext) {
      res.next()
      resCount += 1
      if(resCount/100 == 0) {
        println(resCount)
      }
    }
    resCount
  }
}
