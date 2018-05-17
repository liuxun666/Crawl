package com.My.Crawl

import java.io.{FileWriter, PrintWriter}
import java.util.concurrent._

import com.My.Crawl.Beans.MoocPlatform
import org.json4s.native.Json
import org.json4s._
import org.json4s.native.JsonMethods._
import org.jsoup.Connection.Method
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Random


object jsoupTest {
  def main(args: Array[String]): Unit = {
    val evaluate = "https://www.icourse163.org/web/j/mocCourseV2RpcBean.getCourseEvaluatePaginationByCourseIdOrTermId.rpc?csrfKey=a87cf1db973945dfbe8e717a23d3234b"

    mutilCrawl()
    println("finished! ")

  }

  def mutilCrawl() = {
    //build queue
    val totalCount = 1483
    val pageSize = 20
    val queue = new ArrayBlockingQueue[Int](totalCount / pageSize + 1)
    (1 to totalCount / 20).map(queue.offer)

    val data = mutable.Map("categoryId" -> "-1", "type" -> "30", "orderBy" -> "20", "pageSize" -> s"$pageSize")
    val cookies = Map("EDUWEBDEVICE" -> "70f71211f24a452b958a350b5dfc512a", "NTESSTUDYSI" -> "a87cf1db973945dfbe8e717a23d3234b")
    val platform = "www.icourse163.org"
    val writer = new PrintWriter(new FileWriter("classes.csv"))
    val writerFilshed = new PrintWriter(new FileWriter("doneId.txt"))
    val doneIds = Source.fromFile("doneId.txt").getLines().map(_.toInt).toArray
    val random = new Random(42)
    sys.addShutdownHook({
      writer.synchronized{
        writer.flush()
        writer.close()
      }
      writerFilshed.synchronized{
        writerFilshed.flush()
        writerFilshed.close()
      }

    })

    val threadPool: ExecutorService = Executors.newFixedThreadPool(10)

    while (!threadPool.isTerminated) {
      while(!queue.isEmpty){
        val id = queue.poll()
        if(!doneIds.contains(id)){
          val task = new Runnable {
            override def run(): Unit = {
              try {
                println(s"开始$id")
                crawl(platform, id, data, cookies, writer)
                writerFilshed.synchronized(writerFilshed.println(id))
                println(s"$id 完成")
                Thread.sleep(random.nextInt(3000) + 3000L)
              } catch {
                case e: Exception =>
                  println(s"$id 出错")
                  queue.offer(id)
                  println(e.getMessage)
              }
            }
          }
          threadPool.submit(task)
        }
      }

      Thread.sleep(random.nextInt(2000) + 2000L)
    }
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    writer.close()
    writerFilshed.flush()
    writerFilshed.close()
  }

  def crawl(platform: String, pageId: Int, data: mutable.Map[String, String], cookies: Map[String, String], writer: PrintWriter) = {
    data += ("pageIndex" -> s"$pageId")
    val response = Jsoup.connect("https://www.icourse163.org/web/j/courseBean.getCoursePanelListByFrontCategory.rpc?csrfKey=a87cf1db973945dfbe8e717a23d3234b")
      .data(data.asJava)
//      .cookies(cookies.asJava)
      .ignoreContentType(true)
      .timeout(60000)
      .method(Method.POST)
      .execute()
    val code = response.statusCode()
    if(code == 200){
      val classes = ParseResult(response.body())
      if(classes != null){
        writer.synchronized{
          classes.foreach(lis => writer.println(platform + "," + lis.mkString(",")))
          writer.flush()
        }
      }
    }else throw new RuntimeException("200 error！")

      //TODO save classes with url(platform)
  }

  private def ParseResult(res: String) = {
    val json = parse(res)
    val code = json \ "code"
    if (code.values == 0) {
      val results = json \ "result" \ "result"
      val id = (results \ "id").values.asInstanceOf[List[Any]]
      val name = (results \ "name").values.asInstanceOf[List[Any]]
      val learnercount = (results \ "learnerCount").values.asInstanceOf[List[Any]]
      val learnedcount = (results \ "learnedCount").values.asInstanceOf[List[Any]]
      val schoolName = (results \ "schoolPanel" \ "name").values.asInstanceOf[List[Any]]
      val shortName = (results \ "schoolPanel" \ "shortName").values.asInstanceOf[List[Any]]
      id zip name zip learnedcount zip learnercount zip schoolName zip shortName map {
        case (((((a, b), c), d), e), f) => List(a, b, c, d, e, f)
      }

    } else null
  }




}

