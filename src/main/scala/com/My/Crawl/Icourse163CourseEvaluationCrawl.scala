package com.My.Crawl

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import com.My.Utils.{JDBCUtil, RedisUtil}
import com.github.marklister.collections.immutable.CollSeq4
import org.jsoup.Connection.Method
import org.jsoup.Jsoup
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/18
  * Email: liuzhao@66law.cn
  */
object Icourse163CourseEvaluationCrawl {
  def main(args: Array[String]): Unit = {
    val url = "https://www.icourse163.org/web/j/mocCourseV2RpcBean.getCourseEvaluatePaginationByCourseIdOrTermId.rpc?csrfKey=44d6e14dea89409d983d85440337b093"
    val threadPool: ExecutorService = Executors.newFixedThreadPool(20)
    val processingTaskCount = new AtomicInteger()
    val random = new Random(42)
    while (hasMoreTask() || processingTaskCount.get() > 0){
      if(hasMoreTask()){
        processingTaskCount.incrementAndGet()
        val task = new Runnable {
          override def run(): Unit = {
            process(url)
            processingTaskCount.decrementAndGet()
            Thread.sleep(random.nextInt(2000) +  5000)
          }
        }
        threadPool.submit(task)
      }else{
        Thread.sleep(random.nextInt(2000) +  5000)
      }
    }
  }

  def hasMoreTask() = RedisUtil.exists("pages")

  def getTask() = RedisUtil.rpop("pages")

  def process(url: String, pageSize: String = "20", orderBy: String = "3") = {
    val task = getTask()
    if(task.isDefined){
      println(s"process $task")
      val strings = task.get.split("\001")
      assert(strings.length == 3, s"错误的参数 $strings")

      val platform = strings(0)
      val courseId = strings(1)
      val pageIndex = strings(2)
      try{
        val data = Map("courseId" -> courseId, "pageIndex" -> pageIndex, "pageSize" -> pageSize, "orderBy" -> orderBy)

        val (pageCount, result) = crawl(url, data)
        if(result.nonEmpty){
          val conn = JDBCUtil.pool.getConnection
          conn.setAutoCommit(false)
          val stmt = conn.prepareStatement("insert into courseevaluation (platformname, courseid, content, agreecount, star, username)" +
            "values (?, ?, ?, ?, ?, ?)")
          result.foreach{
            case (agreecount, content, star, user) =>
              stmt.setString(1, platform)
              stmt.setString(2, courseId)
              stmt.setString(3, content)
              stmt.setInt(4, agreecount.intValue())
              stmt.setDouble(5, star)
              stmt.setString(6, user)
              stmt.addBatch()
            case _ =>
          }
          stmt.executeBatch()
          conn.commit()
          conn.close()

        }
        if(data("pageIndex") == "1" && pageCount > 1){
          hasNewPage(platform, courseId, pageCount)
        }
        println(s"success crawl $task")
      }catch {
        case e: Exception =>
          println(s"error $task")
          e.printStackTrace()
          RedisUtil.lpush("pages", task.get)
      }

    }

  }

  def crawl(url: String, data: Map[String, String], cookies: Map[String, String] = Map.empty, method: Method = Method.POST) = {
    val response = Jsoup.connect(url)
      .data(data)
      .cookies(cookies)
      .timeout(60000)
      .ignoreContentType(true)
      .method(method)
      .execute()
    val code = response.statusCode()
    //now just to process 200 only, 300+ ignore
    if(code == 200){
      val json = parse(response.body())
      val pageCount = (json \\ "totlePageCount").values
      val list = json \ "result" \ "list"
      val agreeCount = (list \ "agreeCount").values.asInstanceOf[List[BigInt]]
      val content = (list \ "content").values.asInstanceOf[List[String]]
      val star = (list \ "mark").values.asInstanceOf[List[Double]]
      val userName = (list \ "userNickName" ).values.asInstanceOf[List[String]]
      import com.github.marklister.collections._
      val resultList = agreeCount flatZip content flatZip star flatZip userName
      (pageCount.asInstanceOf[BigInt].intValue(), resultList)
    }else(0, CollSeq4(Nil))

  }

  def hasNewPage(platform: String, classId: String, pageCount: Int) = {
    RedisUtil.lpush("pages", (2 to pageCount).map(platform + "\001" + classId + "\001" + _): _*)
  }
}
