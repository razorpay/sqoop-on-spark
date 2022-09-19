/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.razorpay.spark.jdbc

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.razorpay.spark.jdbc.common.Constants
import org.apache.spark.sql.functions.{col, from_unixtime, lit, substring}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * Class that contains JDBC source, read parallelism params and target table name
 *
 * @param source      - JDBC source table
 * @param destination - Delta target database.table
 * @param splitBy     - column by which to split source data while reading
 * @param chunks      - to how many chunks split jdbc source data
 */
case class ImportConfig(
    scope: String,
    inputTable: String,
    query: Option[String],
    boundaryQuery: Option[String],
    outputTable: String,
    splitBy: Option[String],
    chunks: Int,
    partitionBy: Option[String],
    database: String,
    mapColumns: Option[String],
    maxExecTimeout: Long,
    schema: Option[String]
) {

  val splitColumn: String = splitBy.getOrElse(null.asInstanceOf[String])

  val dbType: String = dbutils.secrets.get(scope = scope, key = "DB_TYPE")

  val escapeCharacter = if (dbType == Constants.MYSQL) {
    "`"
  } else if (dbType == Constants.POSTGRESQL) {
    """""""
  }

  val inputTableEscaped: String = escapeCharacter + inputTable + escapeCharacter

  val boundsSql: String = boundaryQuery.getOrElse(
    s"(select min($splitColumn) as min, max($splitColumn) as max from $inputTableEscaped) as bounds"
  )

  val jdbcQuery: String = query.getOrElse(inputTableEscaped)
}

/**
 * Class that does reading from JDBC source, transform and writing to Delta table
 *
 * @param databricksScope  databricks secret scope  for jdbc source
 * @param importConfig  case class that contains source read parallelism params and target table
 * @param jdbcParams  additional JDBC session params like isolation level, perf tuning,
 *                    net wait params etc.
 */
class JDBCImport(
    databricksScope: String,
    importConfig: ImportConfig,
    jdbcParams: Map[String, String] = Map()
)(implicit val spark: SparkSession) {

  import spark.implicits._

  def createDbIfNotExists(): Unit = {
    val outputDbName = importConfig.outputTable.split("\\.")(0)
    val s3Bucket = dbutils.secrets.get(scope = Constants.SCOPE, key = "S3_BUCKET")
    val baseS3Path = s"s3a://$s3Bucket/sqoop"

    if (!spark.catalog.databaseExists(outputDbName)) {
      spark.sql(
        s"CREATE DATABASE IF NOT EXISTS $outputDbName LOCATION '$baseS3Path/$outputDbName'"
      )
    }
  }

  implicit def mapToProperties(m: Map[String, String]): Properties = {
    val properties = new Properties()
    val jdbcUsername = dbutils.secrets.get(scope = databricksScope, key = "DB_USERNAME")
    val jdbcPassword = dbutils.secrets.get(scope = databricksScope, key = "DB_PASSWORD")

    val dbType = dbutils.secrets.get(scope = databricksScope, key = "DB_TYPE")

    if (dbType == Constants.MYSQL) {
      properties.setProperty("driver", Constants.MYSQL_DRIVER)
    } else if (dbType == Constants.POSTGRESQL) {
      properties.setProperty("driver", Constants.POSTGRESQL_DRIVER)
    }

    properties.setProperty("queryTimeout", Constants.QUERY_TIMEOUT.toString)
    properties.put("user", jdbcUsername)
    properties.put("password", jdbcPassword)

    if (dbType == "mysql") {
      properties.put("tinyInt1isBit", "false")
      properties.put("useSSL", "false")
      // https://www.taogenjia.com/2021/05/26/JDBC-Error-Java-sql-SQLException-Zero-Date-value-Prohibited/
      properties.put("zeroDateTimeBehavior", "convertToNull")
      properties.put("sessionVariables", s"MAX_EXECUTION_TIME=${importConfig.maxExecTimeout}")
    }

    m.foreach(pair => properties.put(pair._1, pair._2))
    properties
  }

  def buildJdbcUrl: String = {
    val host = dbutils.secrets.get(scope = databricksScope, key = "DB_HOST")
    val port = dbutils.secrets.get(scope = databricksScope, key = "DB_PORT")
    val dbType = dbutils.secrets.get(scope = databricksScope, key = "DB_TYPE")

    val database = importConfig.database
    val schema = importConfig.schema

    var connectionUrl = s"jdbc:$dbType://$host:$port/$database"

    if (dbType == Constants.POSTGRESQL && schema.isDefined){
      connectionUrl = s"jdbc:$dbType://$host:$port/$database?currentSchema=${schema.get}"
    }

    connectionUrl
  }

  private lazy val sourceDataframe = readJDBCSourceInParallel()

  /**
   * obtains lower and upper bound of source table and uses those values to read in a JDBC dataframe
   *
   * @return a dataframe read from source table
   */
  private def readJDBCSourceInParallel(): DataFrame = {

    if (importConfig.splitBy.nonEmpty) {
      val defaultString=""
      val (lower, upper) = spark.read
        .jdbc(buildJdbcUrl, importConfig.boundsSql, jdbcParams)
        .selectExpr("cast(min as string) min", "cast(max as string) max")
        .as[(Option[String], Option[String])]
        .take(1)
        .map { case (a, b) => (a.getOrElse(defaultString), b.getOrElse(defaultString)) }
        .head

      val jdbcUsername = dbutils.secrets.get(scope = databricksScope, key = "DB_USERNAME")
      val jdbcPassword = dbutils.secrets.get(scope = databricksScope, key = "DB_PASSWORD")

      spark.read.format("jdbc")
        .option("url",buildJdbcUrl)
        .option("dbtable",importConfig.jdbcQuery)
        .option("user",jdbcUsername)
        .option("password",jdbcPassword)
        .option("partitionColumn",importConfig.splitColumn)
        .option("lowerBound",lower)
        .option("upperBound",upper)
        .option("numPartitions",importConfig.chunks)
        .load()
        .where(s"${importConfig.splitColumn} >= '$lower' and ${importConfig.splitColumn} <= '$upper'")


    } else {
      spark.read.jdbc(buildJdbcUrl, importConfig.jdbcQuery, jdbcParams)
    }
  }

  def resolveColumnDataType(dataType: String): String = {
    if (dataType.contains(Constants.DECIMAL_TYPE)) {
      dataType.replace(Constants.DECIMAL_TYPE, "decimal")
    } else {
      Constants.dataTypeMapping.getOrElse(dataType, Constants.STRING)
    }
  }

  def createHiveTable(df: DataFrame, s3Path: String, outputTable: String): Unit = {
    val partitionColumn = importConfig.partitionBy

    val columnList = if (partitionColumn.isDefined) { df.drop(partitionColumn.get).dtypes }
    else { df.dtypes }

    val columnListResolved = columnList.map(x => {
      val dataType = resolveColumnDataType(x._2)

      s"`${x._1}` $dataType"
    })

    val schema = columnListResolved.mkString(",")

    val dropTableQuery = s"drop table if exists $outputTable"

    var createTableQuery =
      s"CREATE EXTERNAL TABLE $outputTable ($schema) " +
      s"ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
      s"STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
      s"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
      s"LOCATION '$s3Path'"

    if (partitionColumn.isDefined) {
      val partitionColumnDataType = resolveColumnDataType(
        df.select(partitionColumn.get).dtypes(0)._2
      )
      createTableQuery =
        createTableQuery + s" PARTITIONED BY (`${partitionColumn.get}` $partitionColumnDataType)"
    }

    spark.sql(dropTableQuery)
    spark.sql(createTableQuery)

    if (partitionColumn.isDefined) {
      val msckRepairQuery = s"msck repair table $outputTable"
      spark.sql(msckRepairQuery)
    }
  }

  private implicit class DataFrameExtensionOps(df: DataFrame) {

    def writeToParquet(s3Path: String): DataFrame = {
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(s3Path)

      df
    }

    def writeAsPartitioned(s3Path: String, partitionColumn: String): DataFrame = {
      val partitionedDf = partitionColumn match {
        case Constants.CREATED_DATE =>
          if (
            df.columns
              .contains(Constants.CREATED_AT) && !df.columns.contains(Constants.CREATED_DATE)
          ) {
            if (List(IntegerType, LongType).contains(df.schema(Constants.CREATED_AT).dataType)){
//              This means that created_at stores epoch timestamp in either seconds or milliseconds.
              df.withColumn(
                partitionColumn,
                from_unixtime(
                  substring(col(Constants.CREATED_AT), 1, 10).cast(IntegerType) + lit(19800),
                  "yyyy-MM-dd"
                )
              )
            } else {
//              This means that created_at stores data types in YYYY-MM-DD HH:MM:SS format.
//              We dont want to add 19800 seconds here since it is also not added in the
//              entity processor for such datatypes.
              df.withColumn(
                partitionColumn,
                  substring(col(Constants.CREATED_AT), 1, 10)
              )
            }
          } else { df }
        case _ => df
      }

      partitionedDf.write
        .mode(SaveMode.Overwrite)
        .partitionBy(partitionColumn)
        .parquet(s3Path)

      partitionedDf
    }
  }

  /**
   * Runs transform against dataframe read from jdbc and writes it to s3
   */
  def run(): Unit = {
    val df = if (importConfig.mapColumns.nonEmpty) {
      DataTransforms.castColumns(sourceDataframe, importConfig.mapColumns.get)
    } else { sourceDataframe }

    val s3Bucket = dbutils.secrets.get(scope = Constants.SCOPE, key = "S3_BUCKET")
    val dbtable = importConfig.outputTable.split("\\.")

    assert(
      dbtable.size == 2 && dbtable.forall(_.trim.nonEmpty),
      "Please provide the output-table in the format {DB_NAME}.{TABLE_NAME}"
    )

    assert(
      !dbtable.startsWith(Constants.HUDI_DB_PREFIX),
      "Output database name provided is invalid. Please try again"
    )

    val s3Path = s"s3a://$s3Bucket/sqoop/${dbtable(0)}/${dbtable(1)}"

    createDbIfNotExists()

    val finalDf = importConfig.partitionBy match {
      case None                  => df.writeToParquet(s3Path)
      case Some(partitionColumn) => df.writeAsPartitioned(s3Path, partitionColumn)
    }

    createHiveTable(finalDf, s3Path, importConfig.outputTable)
  }
}

object JDBCImport {

  def apply(
      scope: String,
      importConfig: ImportConfig,
      jdbcParams: Map[String, String] = Map()
  )(implicit spark: SparkSession): JDBCImport = {

    new JDBCImport(scope, importConfig, jdbcParams)
  }
}
