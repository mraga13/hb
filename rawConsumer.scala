package com.delta.svoc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
//import org.apache.log4j.Level._
//import org.apache.spark.sql.functions._


trait sparkContextCustom extends customVariables {

  val conf: SparkConf = new SparkConf().setAppName(customAppName).setMaster(customSparkMaster)
  val sc = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .appName(customAppName)
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .getOrCreate()
}

object rawConsumer extends sparkContextCustom {

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.functions._
    spark.sparkContext.setLogLevel("ERROR")
    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)

    //Read from ACS/IFS Raw zone table
    println("===============================================================")
    println("Reading data from ACS/IFS Raw zone table")
    println("===============================================================")
    val df = sql(s"select * from $rawHiveDB.cust_tracking_list")
    //Removing Header in DF
    val header = df.first()
    val dff = df.filter(row => row != header)
    val svocImptCust = dff.selectExpr(
      "AA_SENT_DATE_TIMESTAMP_UTC AS RECEIVED_TS"
      ,"MKD_FLT_NB"
      ,"SCH_DPRT_ARPT_CD"
      ,"SCH_ARR_ARPT_CD"
      ,"SCH_DPRT_LDTTM AS SCHED_LEG_LCL_DPTR_TMS"
      ,"SCHED_MKT_AL_CDE AS OPER_CRR_CD"
      ,"CAST(SCHED_SEG_LCL_DPRT_DT AS DATE) AS LEG_DPTR_DT"
      ,"SCH_ARR_LDDTM"
      ,"EST_LEG_LCL_DPTR_TMS"
      ,"EST_LEG_LCL_ARR_TMS"
      ,"EST_LEG_UTC_DPTR_TMS"
      ,"EST_LEG_UTC_ARR_TMS"
      ,"SCH_ARR_UTCDTTM"
      ,"SCH_DPRT_UTCDTTM AS SCHED_LEG_UTC_DPTR_TMS"
      ,"PSR_CUST_SM_NB"
      ,"PSR_CUST_SM_STT"
      ,"CUST_FST_NM"
      ,"CUST_LST_NM"
      ,"CUST_SEAT_NB"
      ,"CUST_HM_ARPRT_CD"
      ,"BRD_STATS_CDE"
      ,"PSR_PRI_NB"
      ,"RECOG_ID"
      ,"PSR_RCTN_TYP_CD"
      ,"PSR_RSN_CD"
      ,"PSR_RCTN_RSN_TXT"
      ,"AA_SENT_DATE_TIMESTAMP"
      ,"CTL_IND"
      ,"CASE WHEN CTL_IND=1 THEN COUNT(PSR_CUST_SM_NB) OVER (PARTITION BY MKD_FLT_NB,SCH_ARR_ARPT_CD,SCH_DPRT_ARPT_CD,SCHED_SEG_LCL_DPRT_DT,SCHED_MKT_AL_CDE) ELSE 0 END AS IMPACTED_CUST_CNT"
    )
    svocImptCust.cache()
    svocImptCust.show(5)

    //Read from Flight_tmp & GG table
    println("===============================================================")
    println("Reading data from FLT_TMP & Gate Grouping Raw zone tables")
    println("===============================================================")
    //  val flightTmpDF = sql("select flt_nb,trim(dprt_stn_cd) as orig,trim(arr_stn_cd) as dest,trim(dprt_gt_id) as dprt_gt_id,flt_orig_dt from default.flight_history")
    val flightTmpDF = spark.read.format("csv").option("header", "true").load("hdfs://EDL-HDP/dvl/edl/apps/hive/warehouse/flt_mstr.csv")

    val ggDF = sql("select area,gate,station from svoc_gate.gate_grouping")
    //Removing Header in DF
    val ggheader = ggDF.first()
    val ggf = ggDF.filter(row => row != ggheader)
    val gateGroupDF = ggf.select($"area",$"gate",$"station")

    // Join FLT_TMP, Gate Grouping
    // val flt_tmp_mstr = flightTmpDF.alias("ft").join(gateGroupDF.alias("gg"), $"dprt_gt_id" === $"gate" && $"dest" === $"station","inner").select($"ft.*",$"gg.*")
    val flightTmpDFDate = flightTmpDF.withColumn("Flt_Orig_Date",col("Flt_Orig_Dt").cast("date")).drop("Flt_Orig_Dt")
    val flt_tmp_mstr = flightTmpDFDate.alias("ft").join(gateGroupDF.alias("gg"), trim($"dprt_gt_id") === trim($"gate") && trim($"Dprt_Trpn_Stn_Cd") === trim($"station"),"inner").select($"ft.*",$"gg.*")
    flt_tmp_mstr.cache()
    flt_tmp_mstr.show(10)

    // Join SVOC_IMPT_CUST & FLT_TMP_MSTR
    println("===============================================================")
    println("Populating Gate Group column details")
    println("===============================================================")
    val svoc_Gate_Grp = svocImptCust.alias("pctl").join(flt_tmp_mstr.alias("ft"),trim($"mkd_flt_nb") === trim($"Flt_Nb") && trim($"sch_arr_arpt_cd") === trim($"Arr_Trpn_Stn_Cd") && trim($"sch_dprt_arpt_cd") === trim($"Dprt_Trpn_Stn_Cd") && to_date(trim($"LEG_DPTR_DT")) === to_date(trim($"Flt_Orig_Date")),"inner").selectExpr("RECEIVED_TS"
      ,"MKD_FLT_NB"
      ,"SCH_DPRT_ARPT_CD"
      ,"SCH_ARR_ARPT_CD"
      ,"SCHED_LEG_LCL_DPTR_TMS"
      ,"OPER_CRR_CD"
      ,"LEG_DPTR_DT"
      ,"SCH_ARR_LDDTM"
      ,"EST_LEG_LCL_DPTR_TMS"
      ,"EST_LEG_LCL_ARR_TMS"
      ,"EST_LEG_UTC_DPTR_TMS"
      ,"EST_LEG_UTC_ARR_TMS"
      ,"SCH_ARR_UTCDTTM"
      ,"SCHED_LEG_UTC_DPTR_TMS"
      ,"AREA AS GATE_GROUPING_NAME"
      ,"GATE AS GATE_ID"
      ,"PSR_CUST_SM_NB"
      ,"PSR_CUST_SM_STT"
      ,"CUST_FST_NM"
      ,"CUST_LST_NM"
      ,"CUST_SEAT_NB"
      ,"CUST_HM_ARPRT_CD"
      ,"BRD_STATS_CDE"
      ,"PSR_PRI_NB"
      ,"RECOG_ID"
      ,"PSR_RCTN_TYP_CD"
      ,"PSR_RSN_CD"
      ,"PSR_RCTN_RSN_TXT"
      ,"AA_SENT_DATE_TIMESTAMP"
      ,"CTL_IND"
      ,"IMPACTED_CUST_CNT")
    svoc_Gate_Grp.cache()
    svoc_Gate_Grp.show(10)

    //Read from Interaction Feedback table
    println("===============================================================")
    println("Reading data from Interaction Feedback Raw zone table")
    println("===============================================================")
    //    val intFdbkDF = sql("select * from svoc_interation_feedback.interaction_fdbk")
    val intFdbkDF = spark.read.format("csv").option("header", "true").load("hdfs://EDL-HDP/data/svoc/interation_feedback/interaction_fdbk_Mockup.csv")
    val intrFdbkDF = intFdbkDF.selectExpr("MKD_FLT_NB as flight_num"
      ,"SCH_DPRT_ARPT_CD as origin_airport_cd"
      ,"SCH_ARR_ARPT_CD as destination_airport_cd"
      ,"PSR_CUST_SM_NB as loyalty_member_id"
      ,"RESOLUTION_STATUS_CD as resolution_status_cd"
      ,"FEEDBACK_UTCTS as feedback_utcts"
      ,"SCH_DPRT_LDTTM as flight_departure_dt")
    //Removing Header in DF
    //    val ifbheader = intFdbkDF.first()
    //    val ifbf = intFdbkDF.filter(row => row != ifbheader)
    //    val intrFdbkDF = ifbf.select($"flight_num",$"origin_airport_cd",$"destination_airport_cd",$"loyalty_member_id",$"resolution_status_cd",$"feedback_utcts",$"flight_departure_dt")
    intrFdbkDF.cache()

    // Join SVOC_Gate_Grp & FLT_TMP_MSTR
    println("===============================================================")
    println("Populating Gate Group column details")
    println("===============================================================")
    val svoc_Curated = svoc_Gate_Grp.alias("pctl").join(intrFdbkDF.alias("intfb"),$"mkd_flt_nb" === $"flight_num" && $"sch_arr_arpt_cd" === $"origin_airport_cd" && $"sch_dprt_arpt_cd" === $"destination_airport_cd" && $"psr_cust_sm_nb" === $"loyalty_member_id" && to_date($"sched_leg_lcl_dptr_tms") === to_date($"flight_departure_dt"),"left_outer").selectExpr(
      "RECEIVED_TS"
      ,"MKD_FLT_NB"
      ,"SCH_DPRT_ARPT_CD"
      ,"SCH_ARR_ARPT_CD"
      ,"SCHED_LEG_LCL_DPTR_TMS"
      ,"OPER_CRR_CD"
      ,"LEG_DPTR_DT"
      ,"SCH_ARR_LDDTM"
      ,"EST_LEG_LCL_DPTR_TMS"
      ,"EST_LEG_LCL_ARR_TMS"
      ,"EST_LEG_UTC_DPTR_TMS"
      ,"EST_LEG_UTC_ARR_TMS"
      ,"SCH_ARR_UTCDTTM"
      ,"SCHED_LEG_UTC_DPTR_TMS"
      ,"GATE_GROUPING_NAME"
      ,"GATE_ID"
      ,"PSR_CUST_SM_NB"
      ,"PSR_CUST_SM_STT"
      ,"CUST_FST_NM"
      ,"CUST_LST_NM"
      ,"CUST_SEAT_NB"
      ,"CUST_HM_ARPRT_CD"
      ,"BRD_STATS_CDE"
      ,"PSR_PRI_NB"
      ,"RECOG_ID"
      ,"PSR_RCTN_TYP_CD"
      ,"PSR_RSN_CD"
      ,"PSR_RCTN_RSN_TXT"
      ,"AA_SENT_DATE_TIMESTAMP"
      ,"CTL_IND"
      ,"FIRST_VALUE(FEEDBACK_UTCTS) OVER (PARTITION BY MKD_FLT_NB,SCH_ARR_ARPT_CD,SCH_DPRT_ARPT_CD,LEG_DPTR_DT,OPER_CRR_CD,PSR_CUST_SM_NB ORDER BY FEEDBACK_UTCTS DESC) AS FEEDBACK_UTCTS"
      ,"FIRST_VALUE(RESOLUTION_STATUS_CD) OVER (PARTITION BY MKD_FLT_NB,SCH_ARR_ARPT_CD,SCH_DPRT_ARPT_CD,LEG_DPTR_DT,OPER_CRR_CD,PSR_CUST_SM_NB ORDER BY FEEDBACK_UTCTS DESC) AS RESOLUTION_STATUS_CD"
      ,"IMPACTED_CUST_CNT"
    )
    svoc_Curated.cache()
    svoc_Curated.show(10)

    // Check if estimatedUTC > Current DateTS
    // Need to write logic for this

    println("===============================================================")
    println("Calling Curated Load to insert records into SVOC_CTL table")
    println("===============================================================")

    curatedLoad.hiveLoad(svoc_Curated)

    println("===============================================================")
    println("Calling Publish Load to insert records into HBase tables")
    println("===============================================================")

    val tstDF = spark.sql("SELECT * FROM svoc_ctl.svoc_ctl")
    tstDF.show(3)

    val pubDFF = tstDF.selectExpr("operatingcarriercode"
      ,"flightnum"
      ,"originairportcode"
      ,"destinationairportcode"
      ,"flightdeparturedate"
      ,"gategroupingname"
      ,"gateid"
      ,"scheduleddeparturelocalts"
      ,"scheduleddepartureutcts"
      //      ,"'flightlegarrivaldeparturestatus' as flightlegarrivaldeparturestatus"
      ,"estimatedarrivallocalts"
      ,"estimatedarrivalutcts"
      ,"estimateddeparturelocalts"
      ,"estimateddepartureutcts"
      ,"impactedcustomercnt"
      ,"receivedts")
    pubDFF.cache()
    println("===============================================================")
    println("Loading data into SVOC_CTL_FLIGHT HBase table")
    println("===============================================================")

    val tableName = "SVOC_CTL_FLIGHT"
    val columnFamily = "A"
    val schemaFields = "operatingCarrierCode,flightNum,originAirportCode,destinationAirportCode,flightDepartureDate,gateGroupingName,gateId,scheduledDepartureLocalTs,scheduledDepartureUTCTS,flightLegArrivalDepartureStatus,estimatedArrivalLocalTs,estimatedArrivalUTCTS,estimatedDepartureLocalTs,estimatedDepartureUTCTS,ImpactedCustomerCount,ReceivedUTCTS"

    val rkDF = pubDFF.withColumn("rowKey",concat(col("originAirportCode"),lit("|"),col("gateGroupingName"),lit("|"),col("receivedts")))
    val flgLegDF = rkDF.withColumn("epoch_seconds", unix_timestamp(regexp_replace(col("receivedts"),"T"," ")))

    publishLoad.insertRecords(flgLegDF,tableName,columnFamily,schemaFields,16,17)

    println("===============================================================")
    println("Loading data into SVOC_CTL_FLIGHT_LEG_CUST & HIST HBase table")
    println("===============================================================")
    val tableNameCust = "SVOC_CTL_FLIGHT_LEG_CUST"
    val tableNameHist = "SVOC_CTL_FLIGHT_LEG_CUST_HIST"
    val columnFamilyA = "A"
    val schemaFieldsA = "operatingCarrierCode,flightNum,originAirportCode,destinationAirportCode,flightDepartureDate,gateGroupingName,gateId,scheduledDepartureLocalTs,scheduledDepartureUTCTS,flightLegArrivalDepartureStatus,estimatedArrivalLocalTs,estimatedArrivalUTCTS,estimatedDepartureLocalTs,estimatedDepartureUTCTS,ImpactedCustomerCount,ReceivedUTCTS"

    val rkCustDF = pubDFF.withColumn("rowKey",concat(col("flightDepartureDate"),lit("|"),col("flightnum"),lit("|"),col("originAirportCode"),lit("|"),col("destinationairportcode"),lit("|"),col("operatingcarriercode"),lit("|"),col("receivedts")))
    val flgLegCustDF = rkCustDF.withColumn("epoch_seconds", unix_timestamp(regexp_replace(col("estimatedarrivalutcts"),"T"," ")))

    publishLoad.insertRecords(flgLegCustDF,tableNameCust,columnFamilyA,schemaFieldsA,16,17)
    publishLoad.insertRecords(flgLegCustDF,tableNameHist,columnFamilyA,schemaFieldsA,16,17)

    val columnFamilyB = "B"

    val loop1 = tstDF.selectExpr("concat('DL',\"|\",loyaltymemberid) as loyalmember").distinct().collect().map(_.getString(0)).mkString(",").split(",")
    val loop2 = "loyaltyMemberId,loyaltyprogramtiercd,firstname,lastName,homeairportcode,seatid,boarded,interaction,recognitionId,recognitionId,recognitionSentLocalTs,InteractionStatusCd,recognitionTypeCode,recognitionReasonCode,recognitionReasonText,priorityordernumber,ctlindicator".split(",")

    val tmpB = for( i <- loop1; j <- loop2) yield(i + "|" + j)
    val schemaFieldsB = tmpB.mkString(",")

    val pubCustDF = tstDF.selectExpr("loyaltyMemberId"
      ,"loyaltyprogramtiercd"
      ,"firstname"
      ,"lastName"
      ,"' ' as middleName"
      ,"homeairportcode"
      ,"seatid"
      ,"CASE WHEN boardedstatuscd = 'BRD' THEN true ELSE false END as boarded"
      //   ,"'interaction' as interaction"
      ,"rcognitionid as recognitionId"
      ,"recognitionSentLocalTs"
      ,"InteractionStatusCd"
      ,"recognitiontypecd as recognitionTypeCode"
      ,"recognitionreasoncd as recognitionReasonCode"
      ,"recognitionReasonText"
      ,"priorityordernumber"
      ,"ctlindicator as ctl_ind"
      ,"concat(flightDepartureDate,\"|\",flightnum,\"|\",originAirportCode,\"|\",destinationairportcode,\"|\",operatingcarriercode,\"|\",receivedts) as rowkey"
      ,"unix_timestamp(regexp_replace(estimatedarrivalutcts,\"T\",\" \")) as epoch_seconds"
    )

    publishLoad.insertRecords(pubCustDF,tableNameCust,columnFamilyB,schemaFieldsB,16,17)
    publishLoad.insertRecords(pubCustDF,tableNameHist,columnFamilyB,schemaFieldsB,16,17)

    println("===============================================================")
    println("HBase Load completed")
    println("===============================================================")
  }
}

