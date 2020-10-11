import java.io._
import java.util
import java.util.Date

import org.apache.spark.sql._

import scala.tools.scalap.scalax.rules.scalasig.ScalaSigEntryparsers.S
import java.io.PrintWriter

import org.apache.spark.rdd
import scala.collection.mutable.ArrayBuffer
//import com.liferay.twitter.internal.upgrade.TwitterServiceUpgrade
import org.apache.hadoop.util.progressable
import org.apache.hadoop.fs
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.shell.Count
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.q
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter
import twitter4j._
import scala.collection.Javaconversions
import javax.security.auth.login.Configuration
import org.apache.spark.{SparkConf,SparkContext,status,_}
import twitter4j.{Query, Status, Twitter, TwitterFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}
import org.apache.spark.streaming.api.java.JavaReciverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.FileSystem
import java.io.IOException
import java.util.logging.FileHandler
import java.util.logging.Logger
import java.util.logging.Level
import au.com.bytecode.opencsv.CSVWriter
import org.apache.hadoop.conf
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.hive.HiveContext
@throws[IOException]

object batch_processing {
  //Logger class object is created for implementing logging
  private val Logger= lOGGER.getLogger(twittersparkstreaming.getClass.getName)
  def main(args:Array[String]) {
    //creating entry point for spark app
    val sparkConf= new Spark().setMaster(''local[1]'').setAppName(''Streaming Twitter'')
    val sc = new StreamingContext(sparkConf,Seconds(2))
    LOGGER.log(Level.INFO,''Spark configuration is created'')

    //Twitter Authentication credantials
    System.setProperty(''twitter4j.oauth.consumerKey'', ''9HWI95JHLtkfrtsli8c8h8QBx'')
    System.setProperty(''twitter4j.oauth.consumerSecret'',
    ''6rq04n4mxFY1QRGN6bGxo1UvRv0t4Ws01hvJNeEh9IXZOMSm0o'')
    System.setProperty(''twitter4j.oauth.accessToken'',
    ''3650362332-D6yKZiqCwGz4PgN62vsqZtrxo3rh1QJ0AuBS222'')
    System.setproperty(''twitter4j.oauth.accessTokenSecret'',
    lxQ0CucV2UNovuyD55BCfmm1EmJqHkZiCx4jitzsQqGEc'')

    LOGGER.log9Level.INFO, ''Twitter configuration set as properties'')

    val stream:DStream[Status]=TwitterUtils.creatStream(sc,
      None).window(Seconds(10)).filter(_.getLang==''en'')
    //.filetr(.getText.split('' '').filter(_.contains(''Tuesday Thoughts'')))
    val tweets = stream.map{ status =>
      val a=status.getUser.getName
      val b=status.getText
      val c=status.getUser.getScreenName
      val d=status.getCreatedAt.toString
      (a,b,c,d)
    }
    tweets.foreachRDD(
      rdd=>
        if(!rdd.isEmpty())
        {
          //rdd.collect().foreach(println)

          rdd.saveAsObjectFile(''/hdptmp/s0998sjn/trialnn'')
          //csvWriter.writeALL(a)
          //a.foreach(a=>csvWriter.writeNext())
        })
/*val statuses = tweets.map {case(status)=>
//val tags=status.getText.split('' '').filter(_.contains(''#TuesdayThoughts''))
//tags.exists {x=> true }
status.getText
status.getUser.getName
status.getUser.getScreenName
status.getCreatedAT.toString
}*/
    /* tweets.foreachRDD(rdd=>
    val rdd = rawRDD.map(_.value())
    )*/
    stream.foreachRDD(rdd=> {
      val hashTags = rdd.flatmap(status => getText.split(''
      '').filter(_.contains(''#'')))
      hashTags.respartitio(1)
      hashTags.saveAsObjectFile(''/hdptmp/s0998sjn/#trial'')
    })
    /*val :Query = new twitter4j.Query()
    query.setQuery(''#orangearmy'')
    query.setLang(''en'')*/
    //query.setSince(''2019-04-22'')
    //query.setCount(100)
    //val result:QueryResult=twitter.search(query)
    // val tweets=result.getTweets
    sc.start()
    sc.awaitTermination()
  }
}
