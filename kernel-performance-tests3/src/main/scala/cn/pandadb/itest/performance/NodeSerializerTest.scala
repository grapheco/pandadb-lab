package cn.pandadb.itest.performance

import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.NodeSerializer
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


object NodeSerializerTest {
  def main(args: Array[String]): Unit = {
    val nodes = ArrayBuffer[Array[Byte]]()
    for(i <- 0 to 100) {
      nodes.append(NodeSerializer.serialize(genNode()))
    }
    val t1 = System.currentTimeMillis()
    for(i<- 0 to 10000*10000) {
      NodeSerializer.deserializeNodeValue(nodes(i%nodes.length))
    }
    val used = System.currentTimeMillis()-t1
    println(s"used: ${used}")
  }


  def genNode():StoredNodeWithProperty = {
    val id = rand.nextLong()
    val label = rand.nextInt()
    val props = Map[Int, Any](
      0-> contents(rand.nextInt(contents.length))(0),
      1-> contents(rand.nextInt(contents.length))(1),
      2-> contents(rand.nextInt(contents.length))(2),
      3-> contents(rand.nextInt(contents.length))(3),
      4-> contents(rand.nextInt(contents.length))(4),
      5-> contents(rand.nextInt(contents.length))(5),
      6-> contents(rand.nextInt(contents.length))(6)
    )

    new StoredNodeWithProperty(id, Array[Int](label), props)
  }


  val rand = new Random(100)
  val ids = rand.nextLong()

  val csvStr = """2011-06-08|681475102539777|comment|61.95.242.69|Firefox|right|5
                 2011-06-09|681475102539778|comment|49.15.113.168|Firefox|no way!|7
                 2011-06-09|681475102539779|comment|200.13.135.126|Firefox|no way!|7
                 2011-06-08|681475102539780|comment|49.50.113.9|Chrome|About Mahmud of Ghazni, Iran, as well as Pakistan and North-West IAbout Muhammad A|82
                 2011-06-09|681475102539781|comment|61.11.38.90|Internet Explorer|thanks|6
                 2011-06-08|681475102539782|comment|59.178.229.78|Opera|roflol|6
                 2011-06-08|681475102539783|comment|27.113.255.122|Firefox|yes|3
                 2011-06-09|681475102539784|comment|61.11.94.119|Chrome|good|4
                 2011-06-08|681475102539785|comment|49.50.4.150|Firefox|About Muhammad Ali, for his unorthodox fighting style, whicAbout Alan Jackso|76
                 2011-06-09|681475102539786|comment|1.7.48.253|Internet Explorer|About Muhammad Ali, would rock the 1960s. Ali's exAbout Alan Jackson,  an American count|88
                 2011-06-09|681475102539787|comment|1.4.1.94|Chrome|roflol|6
                 2012-08-04|927765707161613|comment|27.97.227.36|Firefox|thanks|6
                 2012-08-04|927765707161614|comment|61.95.224.42|Firefox|ok|2
                 2012-08-04|927765707161615|comment|27.131.213.151|Firefox|fine|4
                 2012-08-04|927765707161616|comment|27.0.60.28|Firefox|great|5
                 2012-08-04|927765707161617|comment|27.50.6.149|Firefox|About Mahmud of Ghazni, navid dynasty who ruled from 997 until hAbout Paraguay|78
                 2012-08-04|927765707161618|comment|103.1.197.103|Firefox|duh|3
                 2012-08-04|927765707161619|comment|49.137.227.82|Firefox|thanks|6
                 2012-08-05|927765707161620|comment|61.11.38.90|Internet Explorer|About Your Body Is a Wonderland, was released in October 2002 as the second singl|81
                 2012-08-04|927765707161621|comment|103.1.6.199|Firefox|About Batman, he police commissioner Jim Gordon, and occasionally the heroine Batgirl.|86
                 2012-08-05|927765707161622|comment|27.113.253.230|Firefox|LOL|3
                 2012-08-04|927765707161623|comment|14.1.117.248|Internet Explorer|thanks|6
                 2012-05-18|892581335072793|comment|1.2.31.208|Firefox|thx|3
                 2012-05-19|892581335072794|comment|1.39.188.46|Internet Explorer|About Mahmud of Ghazni, xtensive empire which covered most of About Europe|74
                 2012-05-18|892581335072795|comment|49.238.55.217|Firefox|maybe|5
                 2012-05-19|892581335072796|comment|200.40.234.54|Firefox|roflol|6
                 2012-05-19|892581335072797|comment|49.143.252.41|Firefox|About Mahmud of Ghazni, , eastern Iran, as well as Pakistan andAbout Hugo Ch|76
                 2012-05-19|892581335072798|comment|101.56.1.10|Chrome|About Heaven Is a Place on Earth,  be Carlisle's signature song because of its success on the charts and its continuAbout European Economic Community,  treaty als|162
                 2012-05-19|892581335072799|comment|61.8.146.139|Firefox|no|2
                 2012-05-19|892581335072800|comment|27.123.219.122|Firefox|thanks|6
                 """
  val contents = csvStr.trim.split("\r\n").map(_.trim.split('|')).toArray

}
