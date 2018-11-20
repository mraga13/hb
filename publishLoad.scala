package com.delta.svoc

//import org.apache.log4j.{Level, LogManager}
import com.delta.svoc.rawConsumer._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName

object publishLoad {

  def putFunc(put: Put, cf: String, colName: String, tsValue: String, colValue: String): Unit = {
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), tsValue.toLong, Bytes.toBytes(colValue))
  }

  def insertRecords(fltLegDF: DataFrame, tableName: String, columnFamily: String, schemaFields: String, rkIndex: Int, timeStpIndex: Int): Unit = {

    val finRDD = fltLegDF.rdd.map(_.mkString(";,;"))
    val conf = HBaseConfiguration.create()
    val hc = new HBaseContext(sc, conf)
    val colNames = schemaFields.split(",")
    hc.bulkPut[String](finRDD,
      TableName.valueOf(tableName),
      (putRecord) => {
        var colIndex = 0
        var put = new Put(Bytes.toBytes(putRecord.split(";,;")(rkIndex)))
        for (colName <- colNames) {
          putFunc(put, columnFamily, colName, putRecord.split(";,;")(timeStpIndex), putRecord.split(";,;")(colIndex))
          colIndex = colIndex + 1
        }
        put
      })
  }
}
