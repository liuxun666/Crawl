package com.My.Crawl

import java.io.{FileWriter, PrintWriter}
import java.net.URL
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicInteger

import com.My.Utils.{HttpUtil, JDBCUtil, RedisUtil}
import org.json4s.native.JsonMethods.parse
import org.jsoup.Jsoup
import com.github.marklister.collections._
import com.vdurmont.emoji.EmojiParser
import org.json4s.DefaultFormats

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/22
  * Email: liuzhao@66law.cn
  */
object CourseraCourseEvaluationCrawl {
  implicit val formats = DefaultFormats
  val redisClasses = "CourseraClasses"
  val redisEvaluation = "evaluationTask"
  def main(args: Array[String]): Unit = {
    val url = "https://www.coursera.org/api/catalogResults.v2?q=bySubdomain&subdomainId=%s&start=0&limit=25&debug=false&fields=debug,courseId,onDemandSpecializationId,specializationId,courses.v1(name,slug,partnerIds),onDemandSpecializations.v1(courseIds,name,slug,partnerIds),specializations.v1(name,shortName,courseIds,partnerIds),partners.v1(name)&includes=courseId,onDemandSpecializationId,specializationId,courses.v1(partnerIds),onDemandSpecializations.v1(partnerIds,primaryCourseIds),specializations.v1(partnerIds,primaryCourseIds)"
//    BuildTasks()
    val threadPool: ExecutorService = Executors.newFixedThreadPool(20)
    val random = new Random(42)
    val processingTaskCount = new AtomicInteger()
    val currentMap = new java.util.concurrent.ConcurrentHashMap[String, Unit]()
    sys.addShutdownHook(RedisUtil.lpush(redisEvaluation, currentMap.keys().asScala.toSeq: _*))

//    while (hasMoreTask(redisClasses) || processingTaskCount.get() > 0){
//      if(hasMoreTask(redisClasses)){
//        val task = new Runnable {
//          override def run(): Unit = {
//            val task = getTask(redisClasses)
//            if(task.isDefined){
//              val fields = task.get.split("\001")
//              val subdomain = fields(0)
//              val start = fields(1).toInt
//              val limit = fields(2).toInt
//              processingTaskCount.incrementAndGet()
//              try {
//                getClassId(url, subdomain, start, limit)
//              } catch {
//                case e: Exception =>
//                  println(s"faild to process Classes ${task.get}")
//                  e.printStackTrace()
//                  findNewTask(redisClasses, subdomain, start, limit)
//              }
//              processingTaskCount.decrementAndGet()
//              Thread.sleep(random.nextInt(2000) +  5000)
//            }
//          }
//        }
//        threadPool.submit(task)
//
//      }else Thread.sleep(random.nextInt(2000) +  5000)
//    }
    findNewTask(redisEvaluation, "iSxVEG07EeW3YxLB1q9I2w", 0, 100)
    findNewTask(redisEvaluation, "RMFRum1BEeWXrA6ju0fvnQ", 0, 100)
    findNewTask(redisEvaluation, "Gtv4Xb1-EeS-ViIACwYKVQ", 0, 100)
    findNewTask(redisEvaluation, "W_mOXCrdEeeNPQ68_4aPpA", 0, 100)
    findNewTask(redisEvaluation, "GdeNrll1EeSROyIACtiVvg", 0, 100)
//    println("begin build EvaluationTask")
//    buildEvaluationTask()
    println("begin process EvaluationTask")
    while (hasMoreTask(redisEvaluation) || processingTaskCount.get() > 0){
      if(hasMoreTask(redisEvaluation)){
        val task = new Runnable {
          override def run(): Unit = {
            val task = getTask(redisEvaluation)
            if(task.isDefined){
              val fields = task.get.split("\001")
              val classId = fields(0)
              val start = fields(1).toInt
              val total = fields(2).toInt
              if(start < total){
                findNewTask(redisEvaluation, classId, start + 100, total)
              }
              processingTaskCount.incrementAndGet()
              currentMap.put(task.get, null)
              try {
                getEvaluaton(classId, start, 100)
              } catch {
                case e: Exception =>
                  println(s"faild to process Evaluation ${task.get}")
                  e.printStackTrace()
                  findNewTask(redisEvaluation, classId, start, total)
              }
              processingTaskCount.decrementAndGet()
              currentMap.remove(task.get)
              Thread.sleep(random.nextInt(2000) +  5000)
            }
          }
        }
        threadPool.submit(task)

      }else Thread.sleep(random.nextInt(2000) +  5000)
    }

    threadPool.shutdown()
    println("done！")
  }

  def getEvaluaton(classId: String, start: Int, limit: Int) = {
    println(s"begin to process class $classId, $start, $limit")
    val url = s"https://www.coursera.org/api/feedback.v1/?q=course&courseId=$classId&feedbackSystem=STAR&ratingValues=1%2C2%2C3%2C4%2C5&categories=generic&start=$start&limit=$limit"
    val text = Jsoup.connect(url)
      .ignoreContentType(true)
      .headers(HttpUtil.getRandomHeader)
      .timeout(60000)
      .get()
      .body()
      .text()
    val json = parse(text)

    val total = (json \ "paging" \ "total").extract[String].toInt
    val next = if(total > start + limit) (json \ "paging" \ "next").extract[String].toInt else 1
    val content = (json \ "elements" \ "comments" \ "generic" \ "definition" \ "value").extract[List[String]].map(f => EmojiParser.removeAllEmojis(f.replace("<co-content><text>", "").replace(" </text></co-content>", "").replace("</text><text>", " ")))
    val star = (json \ "elements" \ "rating" \ "value").extract[List[Double]]
    val user = (json \ "elements" \ "userId").extract[List[Int]]

    val evaluations = content flatZip star flatZip user
    val conn = JDBCUtil.pool.getConnection
    conn.setAutoCommit(false)
    val pstm = conn.prepareStatement("insert into courseevaluation (platformname, courseid, content, star, username) values(?, ?, ?, ?, ?)")
    evaluations.foreach(f => {
      pstm.setString(1, "www.coursera.org")
      pstm.setString(2, classId)
      pstm.setString(3, f._1)
      pstm.setDouble(4, f._2)
      pstm.setString(5, f._3.toString)
      pstm.addBatch()
    })
    val res = pstm.executeBatch()
    conn.commit()
    conn.close()
    if(start == 0 && next < total) findNewTask(redisEvaluation, classId, next, total)

    println(s"finish to process class $classId, $start, $limit")

  }

  def getClassId(url: String, domainId: String, start :Int, limit: Int) = {
    implicit val formats = DefaultFormats
    println(s"begin to process domain $domainId, $start, $limit")
    val _url = url.format(domainId, start, limit)
    val text = Jsoup.connect(_url)
        .ignoreContentType(true)
        .timeout(60000)
        .get()
        .body()
        .text()
    val json = parse(text)
    val total = (json \ "paging" \ "total").extract[String].toInt
    val next = if(total > limit) (json \ "paging" \ "next").extract[String].toInt else 1

    val courses = json \ "linked" \ "courses.v1"
    val ids = (courses \ "id").extract[List[String]]
    val names = (courses \ "name").extract[List[String]]
    val partnerId = (courses \ "partnerIds").extract[List[List[String]]].map(_.head)

    val partners = json \ "linked" \ "partners.v1"
    val _schoolNames = (partners \ "name").extract[List[String]]
    val schoolIds = (partners \ "id").extract[List[String]]
    val _schoolShortNames = (partners \ "shortName").extract[List[String]]
    //schoolId schoolName shortname
    val schoolMap = (schoolIds zip (_schoolNames zip _schoolShortNames)).toMap

    val schoolInfo = partnerId.map(schoolMap(_))
    val schoolNames = schoolInfo.map(_._1)
    val schoolShortNames = schoolInfo.map(_._2)

    val classes = ids flatZip names flatZip schoolNames flatZip schoolShortNames
    val conn = JDBCUtil.pool.getConnection
    conn.setAutoCommit(false)
    val pstm = conn.prepareStatement("insert into classes (platform, classid, classname, school, schoolshort) values(?, ?, ?, ?, ?)")
    classes.foreach(f => {
      pstm.setString(1, "www.coursera.org")
      pstm.setString(2, f._1)
      pstm.setString(3, f._2)
      pstm.setString(4, f._3)
      pstm.setString(5, f._4)
      pstm.addBatch()
    })
    val res = pstm.executeBatch()
    conn.commit()
    conn.close()
    if(start == 0 && limit < total) findNewTask(redisClasses, domainId, next, total)

    println(s"finish to process $domainId, $start, $limit")
  }

  def hasMoreTask(key: String) = {
    RedisUtil.exists(key)
  }

  /**
    * 如果当前 domainId 还有其他class，加入到redis队列中
    * @param domainId
    * @param start
    * @param end
    * @return
    */
  def findNewTask(key: String, domainId: String, start: Int, end: Int) = {
    RedisUtil.lpush(key, domainId + "\001" + start + "\001" + end)
  }

  /**
    * 获取所有的subdomian,加入到redis中
    * @return
    */
  def BuildTasks() = {
    val text =
      """
        |[{"data":{"DomainsV1Resource":{"domains":{"elements":[{"id":"arts-and-humanities","name":"艺术与人文","description":"艺术与人文专项课程（包括美术、历史和哲学）探讨创造性工作的历史，让您学会批判性审阅原始材料，找出不同观点间的联系，对论据和论点做出判断。该领域中的课程将让您成为一名更好的读者、思考者、艺术家和作家。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/arts-and-humanities.r1.png","subdomains":{"elements":[{"id":"history","name":"历史","domainId":"arts-and-humanities","description":"历史课程探讨古代和现代发生的重大事件和社会趋势。学生不仅要学习战争、帝国主义和全球化等专题课程，还要学习黑人历史和女性历史等针对特定群体或时期的课程。","__typename":"SubdomainsV1"},{"id":"music-and-art","name":"音乐与艺术","domainId":"arts-and-humanities","description":"音乐与艺术课程培育学生对视觉艺术、音乐和创意写作的欣赏和实践能力。学习演奏吉他、讨论当代漫画小说的优缺点、或者探索了解人类的创意历史。","__typename":"SubdomainsV1"},{"id":"philosophy","name":"哲学","domainId":"arts-and-humanities","description":"哲学课程关注人类自身的宏观问题，在现代和历史背景下探讨道德、伦理、目的和理性。介绍特点东西方传统哲学学派，例如存在主义和人道主义。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"business","name":"商务","description":"商务专项课程提高您在现代工作中所需的重要技能，具体内容包括创业、商业战略、营销、金融和管理。无论您是一位小企业主，还是在大型跨国公司工作，商务课程都将提升您分析、理解和解决商业问题的能力。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/business.r1.png","subdomains":{"elements":[{"id":"leadership-and-management","name":"领导与管理","domainId":"business","description":"领导和管理课程面向初次或长期担任领导岗位的学生，帮助他们促进雇员发展，激励和领导团队，针对变化进行调整，以及对整个组织的相关人士施加影响力。 ","__typename":"SubdomainsV1"},{"id":"finance","name":"金融","domainId":"business","description":"金融课程内容包括银行、会计、金融管理、记账、企业金融和金融分析。高级主题包括金融工程、法律财会和资产定价。","__typename":"SubdomainsV1"},{"id":"marketing","name":"营销","domainId":"business","description":"营销课程介绍在整个消费者周期中影响消费者的战略。学生将学习基础知识，例如制定营销战略，塑造品牌，分配广告预算，以及使用数字化方式和社交媒体渠道来实现商业目标。","__typename":"SubdomainsV1"},{"id":"entrepreneurship","name":"创业","domainId":"business","description":"创业课程面向所有希望创业和不断发展小企业的学生。学生将学习创业相关理论和实践，公益创业框架，以及如何培育创新文化使企业保持领先。 ","__typename":"SubdomainsV1"},{"id":"business-essentials","name":"商务核心","domainId":"business","description":"商务核心课程介绍日常商务工作基本知识。具体内容包括商务写作、演讲技巧、交流、商务礼节、有效沟通。无论您是初涉职场，还是希望提升商务技能，这些课程都能够助您一臂之力。","__typename":"SubdomainsV1"},{"id":"business-strategy","name":"商业战略","domainId":"business","description":"商务战略课程将讲授增长模式，以及如何进行明智决策以达成长期商业目标。课程面向商业分析师，商业开发专家，以及任何对商业创新感兴趣的学生。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"computer-science","name":"计算机科学","description":"计算机科学专项课程教授软件工程和设计、算法思维、人机交互、变成语言和计算机历史。本类课程涉及领域比较宽泛，它们将帮助您提高抽象思维能力，系统性地解决问题，提出合理的解决方案。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/computer-science.r1.png","subdomains":{"elements":[{"id":"software-development","name":"软件开发","domainId":"computer-science","description":"软件开发专项课程将介绍开发软件的过程，内容包括开发工具和方法论（例如敏捷开发），程序开发语言（包括 Python、Java 和 Scala），以及软件架构和测试。","__typename":"SubdomainsV1"},{"id":"mobile-and-web-development","name":"移动和网络开发","domainId":"computer-science","description":"移动和网络开发课程将介绍创建网络应用及 Android 和 iOS 原生移动应用的技能。学习 HTML/CSS 和最新框架；PHP、JavaScript、Python 以及其他程序开发语言；以及最新的后台技术。","__typename":"SubdomainsV1"},{"id":"algorithms","name":"算法","domainId":"computer-science","description":"算法课程将帮助您学习解题计算过程，并且在软件中实现计算过程。您将学习设计搜索、排序和优化等算法，并且使用算法解决实际问题。","__typename":"SubdomainsV1"},{"id":"computer-security-and-networks","name":"计算机安全和网络","domainId":"computer-science","description":"计算机安全和网络课程介绍构建现代安全的软硬件基础。具体内容包括网路安全、风险管理和密码学。","__typename":"SubdomainsV1"},{"id":"design-and-product","name":"设计和产品","domainId":"computer-science","description":"设计和产品课程将介绍如何创建软件产品。课程内容主要包括产品定义和设计、产品管理和平面设计；具体课程主题包括交互设计、网页设计、人机交互和设计思维。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"data-science","name":"数据科学","description":"数据科学专项课程内容包括解读数据、分析数据和提出可操作性意见相关的基础知识。初学者和高水平学习者都能找到合适课程，例如定量和定性数据分析、数据处理的工具和方法、机器学习算法。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/data-science.r1.png","subdomains":{"elements":[{"id":"data-analysis","name":"数据分析","domainId":"data-science","description":"数据分析课程介绍管理和分析大规模数据的方法。您将学习数据挖掘、大数据应用以及数据产品开发，成为一名数据科学家。","__typename":"SubdomainsV1"},{"id":"machine-learning","name":"机器学习","domainId":"data-science","description":"机器学习课程介绍如何创建使用和分析大规模数据的系统。具体内容包括预测算法、自然语言处理以及统计模式识别。","__typename":"SubdomainsV1"},{"id":"probability-and-statistics","name":"概率论与数理统计","domainId":"data-science","description":"概率论与数理统计课程介绍理解数据意义的技能，具体内容包括优化、推断、测验、数据模式分析方法，以及使用分析方法来预测、理解和改进结果。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"information-technology","name":"信息技术","description":"信息技术 (IT) 课程和专项课程教授云计算、网络安全、数据管理和网络等内容。学习使用计算机满足日常商务需求，开展或促进您在 IT 行业的职业生涯。","backgroundImageUrl":"https://s3.amazonaws.com/coursera_assets/growth_discovery/images/domains/information-technology.r1.png","subdomains":{"elements":[{"id":"cloud-computing","name":"云计算","domainId":"information-technology","description":"云计算课程和专项课程教授云架构、服务以及托管等内容。通过学习如何正确利用云，让自己在 IT 行业中脱颖而出。","__typename":"SubdomainsV1"},{"id":"security","name":"安全","domainId":"information-technology","description":"安全","__typename":"SubdomainsV1"},{"id":"data-management","name":"数据管理","domainId":"information-technology","description":"数据管理","__typename":"SubdomainsV1"},{"id":"networking","name":"网络","domainId":"information-technology","description":"网络课程和专项课程教授网络管理、架构、基础设施以及故障排除等内容。通过学习网络技能进军 IT 行业。","__typename":"SubdomainsV1"},{"id":"support-and-operations","name":"支持和运营","domainId":"information-technology","description":"支持和运营","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"life-sciences","name":"生命科学","description":"生命科学专项课程探讨生物体和生态系统的性质，具体内容包括生物学、营养学、动物学和保健。本类课程将让您加强对动物和植物的理解，提高分析复杂系统中个体交互和应对改变方式的能力。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/life-sciences.r1.png","subdomains":{"elements":[{"id":"animals-and-veterinary-science","name":"动物和兽医科学","domainId":"life-sciences","description":"动物与兽医课程探讨野生和家养动物习性，以及如何照料它们。课程内容将覆盖全世界动物群的各个方面，例如野生动物保护、动物学、宠物营养和农场动物。","__typename":"SubdomainsV1"},{"id":"bioinformatics","name":"生物信息学","domainId":"life-sciences","description":"生物信息学专项课程介绍用于分析和解读生物数据的工具和概念。您将学习计算机和数据科学的概念应用到基因、微生物和生物化学领域，学习内容还包括相关软件和研究方法。","__typename":"SubdomainsV1"},{"id":"biology","name":"生物","domainId":"life-sciences","description":"生物课程介绍生物体相关研究的基本概念，包括细胞和分子基础，植物和海洋生物，以及进化生物学和计算生物学等进阶主题。 ","__typename":"SubdomainsV1"},{"id":"medicine-and-healthcare","name":"医疗和保健","domainId":"life-sciences","description":"医学和保健课程将拓展您对临床保健、公共卫生和流行病学的理解。具体包括卫生保健工作沟通和全球健康模式等内容。","__typename":"SubdomainsV1"},{"id":"nutrition","name":"营养","domainId":"life-sciences","description":"营养课程介绍食物和健康之间的关系，具体内容包括儿童营养、健康和健身营养，饮食文化传统及其影响。","__typename":"SubdomainsV1"},{"id":"clinical-science","name":"临床科学","domainId":"life-sciences","description":"临床科学课程介绍医疗相关研究及进展，这些研究一般是在实验室环境下进行的。您将掌握医疗研究、医疗科学、流行病学和临床医学工作中需要使用的各种概念。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"math-and-logic","name":"数学和逻辑","description":"在专项课程中，您将学习使用数学和逻辑知识解决定量和抽象问题。您需要完成逻辑试题，学习计算技能，抽象描述真实世界的各种现象，以及加强推理能力。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/math-and-logic.r1.png","subdomains":{"elements":[{"id":"math-and-logic","name":"数学和逻辑","domainId":"math-and-logic","description":"在专项课程中，您将学习使用数学和逻辑知识解决定量和抽象问题。您需要完成逻辑试题，学习计算技能，抽象描述真实世界的各种现象，以及加强推理能力。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"personal-development","name":"个人发展","description":"个人发展专项课程介绍个人成长的战略和框架、目标设定以及自我改进。您将学习个人金融管理，发表印象深刻的演讲，进行道德决策，以及创造性思维。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/personal-development.r1.png","subdomains":{"elements":[{"id":"personal-development","name":"个人发展","domainId":"personal-development","description":"个人发展专项课程介绍个人成长的战略和框架、目标设定以及自我改进。您将学习个人金融管理，发表印象深刻的演讲，进行道德决策，以及创造性思维。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"physical-science-and-engineering","name":"物理科学与工程","description":"物理科学和工程专项课程向学生讲解周围世界的性质，内容从物理和化学核心概念，直到工程中实际应用主题。如果您希望从事电子、土木或机械工程工作，或者进行相关研究和应用，本类课程将为您打下基础。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/physical-science-and-engineering.r1.png","subdomains":{"elements":[{"id":"electrical-engineering","name":"电子工程","domainId":"physical-science-and-engineering","description":"电子工程课程将介绍如何使用电子学来创建、传输和控制信息。您将会首先掌握电路和信号处理基本知识，然后再学习更深入的主题内容，例如微电子学、电信学，电力和控制工程。","__typename":"SubdomainsV1"},{"id":"mechanical-engineering","name":"机械工程","domainId":"physical-science-and-engineering","description":"机械工程课程训练学生设计和制造机械系统，例如在汽车、航空、机器人和制造领域所使用的机械系统。具体内容包括机械学、流体动力学和热传递等。","__typename":"SubdomainsV1"},{"id":"chemistry","name":"化学","domainId":"physical-science-and-engineering","description":"化学课程探讨物质性质及其相互反应。具体内容包括有机和无机化学；工业和研究应用；食品、保健和环境化学。","__typename":"SubdomainsV1"},{"id":"environmental-science-and-sustainability","name":"环境科学与持续发展","domainId":"physical-science-and-engineering","description":"环境科学和可持续发展课程介绍如何平衡人类短期需要和环境长期健康。具体内容包括环境保护、环境政策、农业、可持续性、污染和气候变化。","__typename":"SubdomainsV1"},{"id":"physics-and-astronomy","name":"物理与天文学","domainId":"physical-science-and-engineering","description":"物理与天文学探讨运动物体的动力学，以及行星、恒星、卫星和小行星的特性。具体内容包括天体物理学、宇宙历史和理论和应用物理学","__typename":"SubdomainsV1"},{"id":"research-methods","name":"研究方法","domainId":"physical-science-and-engineering","description":"研究方法课程将介绍如何进行有效且符合伦理的研究。您将学习相关框架和工具，从而能在心理学、社会学和商业研究等领域进行定性和定量研究。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"social-sciences","name":"社会科学","description":"社会科学专项课程探讨人们如何制定法律，做出决策，进行群体行动，以及组织结构化社区。通过教育学、经济学和心理学课程，学生将加强对个人和集体关系的理解，学生将掌握分析行为和趋势的能力。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/social-sciences.r1.png","subdomains":{"elements":[{"id":"economics","name":"经济学","domainId":"social-sciences","description":"经济学课程探讨个人和团体如何管理资源，以及应对资源稀缺的情况。具体内容包括管理经济学、经济政策、国际经济学和经济发展。","__typename":"SubdomainsV1"},{"id":"education","name":"教育","domainId":"social-sciences","description":"教育课程将从应用和理论两个方面介绍教学实践。其内容包括教育政策、教育科技、中小学教育（K-12），以及教师培训","__typename":"SubdomainsV1"},{"id":"governance-and-society","name":"政府与社会","domainId":"social-sciences","description":"政府与社会课程介绍在正常和非常情况下，政府职能以及个人和社会行为。具体内容包括国际关系、政策法规、犯罪学以及种族和民族关系。","__typename":"SubdomainsV1"},{"id":"law","name":"法律","domainId":"social-sciences","description":"法律课程介绍法律和法规系统的历史和解读，具体内容包括刑法和民法、环境法、国际法和宪法。","__typename":"SubdomainsV1"},{"id":"psychology","name":"心理学","domainId":"social-sciences","description":"心理学课程探讨人类的心智情况，以及心智情况如何影响我们的行为。心理学包括法医心理学、儿童心理学、行为心理学以及心理学研究。","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"},{"id":"language-learning","name":"语言学习","description":"在语言课程和专项课程中，您要学习使用全球主要语言，包括英语、汉语、西班牙语等有效地进行听说读写。无论您是学习一门外语，还是提高母语的沟通能力，本领域的课程都将帮助您了解语法和句法，并让您在商务会谈和随意性会话中放任自如。","backgroundImageUrl":"https://coursera_assets.s3.amazonaws.com/growth_discovery/images/domains/language-learning.r1.png","subdomains":{"elements":[{"id":"learning-english","name":"学习英语","domainId":"language-learning","description":"","__typename":"SubdomainsV1"},{"id":"other-languages","name":"其他语言","domainId":"language-learning","description":"","__typename":"SubdomainsV1"}],"__typename":"SubdomainsV1Connection"},"__typename":"DomainsV1"}],"__typename":"DomainsV1Connection"},"__typename":"DomainsV1Resource"}}}]
      """.stripMargin
    val json = parse(text)
    val elements = json \ "data" \ "DomainsV1Resource" \ "domains" \ "elements" \ "subdomains" \ "elements"
    val ids = (elements \ "id").values.asInstanceOf[List[String]]
//    val names = (elements \ "name").values.asInstanceOf[List[String]]
    //    val domainIds = (elements \ "domainId").values.asInstanceOf[List[String]]
    val redisValues = ids.map(_ + "\001" + 0 + "\001" + 25)
    RedisUtil.lpush(redisClasses, redisValues: _*)
  }

  def buildEvaluationTask() = {
    val conn = JDBCUtil.pool.getConnection
    val stmt = conn.createStatement()
    val set = stmt.executeQuery("SELECT classid FROM crawl.classes where platform='www.coursera.org' ")
    val classIds = ArrayBuffer[String]()
    while (set.next()){
      classIds += set.getString(1)
    }
    RedisUtil.lpush(redisEvaluation, classIds.map(_ + "\001" + 0 + "\001" + 10): _*)
    conn.close()

  }

  def getTask(key: String) = {
    RedisUtil.rpop(key)
  }

}
