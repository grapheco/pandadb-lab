import org.apache.logging.log4j.scala.Logging

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created in 18:01 2021/1/18
 * @Modified By:
 */
object Utils extends Logging{
  def timing[T](f: => T): T = {
    val t1 = System.currentTimeMillis()
    val t = f
    val t2 = System.currentTimeMillis()
    logger.info(s"${t2 - t1} ms.")
    t
  }

  def timingByNanoSec[T](f: => T): T = {
    val t1 = System.nanoTime()
    val t = f
    val t2 = System.nanoTime()
    logger.info(s"${t2 - t1} ns.")
    t
  }

}
