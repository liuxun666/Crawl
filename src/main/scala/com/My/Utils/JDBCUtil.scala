package com.My.Utils

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/17
  * Email: liuzhao@66law.cn
  */
object JDBCUtil {
  System.setProperty("com.mchange.v2.c3p0.cfg.xml","conf/c3p0-config.xml")
  /**
    * 218 question库
    */
  lazy val questionPool = new ComboPooledDataSource("question")
  /**
    * 218 artic库
    */
  lazy val articlePool = new ComboPooledDataSource("article")
  /**
    * 218 selfmedia库
    */
  lazy val selfmediaPool = new ComboPooledDataSource("selfmedia")
  /**
    * 218 bigdata库
    */
  lazy val pool = new ComboPooledDataSource()
  /**
    * 192.168.11.10 mysql库
    */
  lazy val mysql = new ComboPooledDataSource("mysql")
  /**
    * 218 proxy库
    */
  lazy val proxy = new ComboPooledDataSource("proxy")
}
