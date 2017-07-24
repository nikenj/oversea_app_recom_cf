package com.oversea.similar

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by niejiabin on 2017/7/13.
  */
object item_sim extends Serializable {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val configFile = args(0).toString
    val config = ConfigFactory.parseFile(new File(configFile))
    val word2vc_similar_dir = config.getString("word2vc_similar_dir")
    val combine_recom_app_table = config.getString("combine_recom_table_out")
    val cf_recom_app_table = config.getString("cf_recom_app_table_out")
    val saveNumber = config.getInt("save_number")
    val operEvent = config.getInt("oper_event")
    val statdate = args(1).toString
    val saveCfResult = config.getBoolean("save_cf_result")
    val repartationNo = config.getInt("repartation_no")
    val normalEnable = config.getBoolean("normal_enable")
    val statTime = DateTime.parse(statdate, DateTimeFormat.forPattern("yyyyMMdd"))
    //    val stat_date = config.getString("stat_date")
    val sparkconf = new SparkConf().setAppName("app_sim_compute")
    val sc = new SparkContext(sparkconf)
    val hivecontext = new HiveContext(sc)
    val statDate = statTime.minusDays(1).toString("yyyyMMdd")

    val sql_search_outer_app = s"select imei as umid, b.pak_id as pak_id, a.oper_time from uxip.dwd_uxip_hotapps_operate_detail_d as a, uxip.ads_rpt_uxip_hotapps_relate_suggest_d as b where" +
      s" a.pak_id=b.pak_id and a.oper_event=$operEvent and (b.app_category_id>=1 and b.app_category_id<=33) and a.stat_date<= $statDate  and b.stat_date= $statDate "
    val sql_search_outer_game = s"select imei as umid, b.pak_id as pak_id, a.oper_time from uxip.dwd_uxip_hotapps_operate_detail_d as a, uxip.ads_rpt_uxip_hotapps_relate_suggest_d as b where" +
      s" a.pak_id=b.pak_id and a.oper_event=$operEvent and (b.app_category_id>=34 and b.app_category_id<=50) and a.stat_date<= $statDate  and b.stat_date= $statDate"

    val user_app_time_rdd: RDD[(String, String, String)] = hivecontext.sql(sql_search_outer_app).map(r => (r.getString(0), r.getString(1), r.getString(2)))
    val user_games_time_rdd: RDD[(String, String, String)] = hivecontext.sql(sql_search_outer_game).map(r => (r.getString(0), r.getString(1), r.getString(2)))
    val word2vc_similar_oper_dir = word2vc_similar_dir + "_" + operEvent
    val word2vc_similar_path = word2vc_similar_oper_dir + "/dt=" + statDate
    val word2vc_similar_rdd: RDD[(String, String, String)] = sc.textFile(word2vc_similar_path).filter(_.nonEmpty).map(_.split(",")).map(r => (r(0), r(1), r(2)))
    val app_similar_rdd = compute_similar(user_app_time_rdd,normalEnable)
    val game_similar_rdd = compute_similar(user_games_time_rdd,normalEnable)
    val all_item_similar_rdd: RDD[(String, String, Double)] = app_similar_rdd.union(game_similar_rdd)
    all_item_similar_rdd.cache()
    val combineSimilarityRdd: RDD[(String, String, Double)] = combine_similar(word2vc_similar_rdd, all_item_similar_rdd)
    save_data(hivecontext, combine_recom_app_table, combineSimilarityRdd, saveNumber, statDate, "temp_table_1", repartationNo)
    if (saveCfResult) save_data(hivecontext, cf_recom_app_table, all_item_similar_rdd, saveNumber, statDate, "temp_table_2", repartationNo)
  }

  /**
    * 用于计算CF-item相关度
    */
  def compute_similar(itemData: RDD[(String, String, String)], normalSwitch: Boolean): RDD[(String, String, Double)] = {
    println(DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "***********************compute similarity start*****************************")
    val userItem = itemData.map(r => (r._1, r._2))
    //    val itemCount: RDD[(String, Int)] = itemData.map(r=>(r._2,1)).reduceByKey(_+_)
    val userDataCombine = userItem.join(userItem)
    val itemCombineFrequent: RDD[((String, String), Int)] = userDataCombine.map(r => (r._2, 1)).reduceByKey(_ + _)
    val itemCombineFilter = itemCombineFrequent.filter(tup => tup._1._1 != tup._1._2)
    val itemCount = itemCombineFrequent.filter(tup => tup._1._1 == tup._1._2).map(tup => (tup._1._1, tup._2))
    val itemFreFirstData = itemCombineFilter.map {
      row =>
        (row._1._1, row)
    }
    val joinItemAndItemcomb: RDD[(String, (((String, String), Int), Int))] = itemFreFirstData.join(itemCount)
    val itemFreSecondData: RDD[(String, ((String, Int, Int), Int))] = joinItemAndItemcomb.map {
      case (firstItemkey, (((_, secondItem), combineNo), firstItemNo)) =>
        (secondItem, (firstItemkey, combineNo, firstItemNo))
    }.join(itemCount)

    val itemSimilar = itemFreSecondData.map {
      case (secondItem, ((firstItem, combineNo, firstItemNo), secondItemNo)) =>
        val similarValue = combineNo / math.sqrt(firstItemNo * secondItemNo)
        (firstItem, (secondItem, similarValue))
    }
    val itemSimilarMax = itemSimilar.map {
      case (firstItem, (_, similarValue)) =>
        (firstItem, similarValue)
    }.reduceByKey {
      (a, b) =>
        if (a > b) a else b
    }
    val itemSimilarNormal =
      if (normalSwitch) {
        itemSimilar.join(itemSimilarMax).map {
          case (firstItem, ((secondItem, similarValue), maxsimilar)) =>
            val normalValue = similarValue / maxsimilar
            (firstItem, secondItem, normalValue)
        }
      } else {
        itemSimilar.map {
          case (firstItem, (secondItem, similarValue)) =>
            (firstItem, secondItem, similarValue)
        }
      }
    println(DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "***********************compute similarity finish*****************************")
    itemSimilarNormal
  }

  /** 将word2vec 计算出的相关度和cf-item的做结合 */
  private def combine_similar(word2vcrdd: RDD[(String, String, String)], allitemsimilarrdd: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {
    val appuniongamesRdd = allitemsimilarrdd.map {
      case (keyitem, relateitem, similarity) =>
        ((keyitem, relateitem), similarity)
    }
    val word2vcRdd = word2vcrdd.map {
      case (keyitem, relateitem, similarity) =>
        ((keyitem, relateitem), similarity.toDouble)
    }
    //    : RDD[((String, String), (String, Option[String]))]
    val combineSimilarData = word2vcRdd.leftOuterJoin(appuniongamesRdd).map {
      case ((keyitem, relateitem), (word2vcsimilarity, cfsimilarity)) =>
        val cfSimilarity = cfsimilarity.getOrElse(word2vcsimilarity)
        //        val word2vcSim = word2vcsimilarity
        val combineSimilarity = (word2vcsimilarity + cfSimilarity) / 2
        (keyitem, relateitem, combineSimilarity)
    }
    combineSimilarData
  }

  /** 将结果按照相关度排序，并且存储到hive表中 */
  def save_data(hivecontext: HiveContext, table_out: String, sim_app_rdd: RDD[(String, String, Double)], save_no: Int, statdate: String, temp_table: String, repartation_no: Int): Unit = {
    println(DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "***********************save data start*****************************")

    val simAppRdd = sim_app_rdd.map {
      case (keyItem, relateItem, similarity) =>
        (keyItem, (relateItem, similarity))
    }.groupByKey().mapValues {
      iter =>
        val arr = iter.toArray
        val sim_arr_tupel = arr.sortBy(_._2).reverse.take(save_no)
        //        val sim_arr_tupel = arr.sortWith(_._2 >= _._2).take(save_no)
        val sim_arr = sim_arr_tupel.map(_._1)
        sim_arr.mkString(",")
    }

    val candidate_rdd = simAppRdd.map(r => Row(r._1, r._2))

    val candidate_Rdd = if (repartation_no > 0) candidate_rdd.repartition(repartation_no) else candidate_rdd
    val structType = StructType(
      StructField("items", StringType, nullable = false) ::
        StructField("simitems", StringType, nullable = false) ::
        Nil
    )
    val candidate_df = hivecontext.createDataFrame(candidate_Rdd, structType)
    candidate_df.registerTempTable(temp_table)
    val create_table_sql: String = s"create table if not exists  $table_out (items String, simitems String) partitioned by (stat_date bigint) stored as textfile"
    val insertInto_table_sql: String = s"insert overwrite table  $table_out  partition(stat_date = $statdate) select * from $temp_table"

    hivecontext.sql(create_table_sql)
    println(DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "***********************inster data start*****************************")
    hivecontext.sql(insertInto_table_sql)
    println(DateTime.now().toString("yyyy-MM-dd HH:mm:ss") + "***********************save data finish*****************************")
  }
}
