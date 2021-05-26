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
import org.apache.spark.sql.functions.{col, from_unixtime, lit}
import org.apache.spark.sql.types.IntegerType
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
    inputTable: String,
    query: Option[String],
    boundaryQuery: Option[String],
    outputTable: String,
    splitBy: Option[String],
    chunks: Int,
    partitionBy: Option[String],
    database: String,
    mapColumns: Option[String]
) {
  val splitColumn: String = splitBy.getOrElse(null.asInstanceOf[String])

  val boundsSql: String = boundaryQuery.getOrElse(
    s"(select min($splitColumn) as min, max($splitColumn) as max from $inputTable) as bounds"
  )

  val jdbcQuery: String = query.getOrElse(inputTable)
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

  implicit def mapToProperties(m: Map[String, String]): Properties = {
    val properties = new Properties()
    val jdbcUsername = dbutils.secrets.get(scope = databricksScope, key = "DB_USERNAME")
    val jdbcPassword = dbutils.secrets.get(scope = databricksScope, key = "DB_PASSWORD")

    val dbType = dbutils.secrets.get(scope = databricksScope, key = "DB_TYPE")

    if (dbType == "mysql") {
      properties.setProperty("driver", Constants.MYSQL_DRIVER)
    } else if (dbType == "postgresql") {
      properties.setProperty("driver", Constants.POSTGRESQL_DRIVER)
    }

    properties.setProperty("queryTimeout", Constants.QUERY_TIMEOUT.toString)
    properties.put("user", jdbcUsername)
    properties.put("password", jdbcPassword)

    m.foreach(pair => properties.put(pair._1, pair._2))
    properties
  }

  def buildJdbcUrl: String = {
    val host = dbutils.secrets.get(scope = databricksScope, key = "DB_HOST")
    val port = dbutils.secrets.get(scope = databricksScope, key = "DB_PORT")
    val dbType = dbutils.secrets.get(scope = databricksScope, key = "DB_TYPE")

    val database = importConfig.database

    val connectionUrl = s"jdbc:$dbType://$host:$port/$database"

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
      val (lower, upper) = spark.read
        .jdbc(buildJdbcUrl, importConfig.boundsSql, jdbcParams)
        .selectExpr("cast(min as long) min", "max")
        .as[(Option[Long], Option[Long])]
        .take(1)
        .map { case (a, b) => (a.getOrElse(0L), b.getOrElse(0L)) }
        .head

      spark.read
        .jdbc(
          buildJdbcUrl,
          importConfig.jdbcQuery,
          importConfig.splitColumn,
          lower,
          upper,
          importConfig.chunks,
          jdbcParams
        )
        .where(s"${importConfig.splitColumn} >= $lower and ${importConfig.splitColumn} <= $upper")
    } else {
      spark.read.jdbc(buildJdbcUrl, importConfig.inputTable, jdbcParams)
    }
  }

  private implicit class DataFrameExtensionOps(df: DataFrame) {

    def writeToParquet(outputTable: String): Unit = {
      df.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(outputTable)
    }

    def writeAsPartitioned(outputTable: String, partitionColumn: String): Unit = {
      val partitionedDf = partitionColumn match {
        case Constants.CREATED_DATE =>
          if (
            df.columns
              .contains(Constants.CREATED_AT) && !df.columns.contains(Constants.CREATED_DATE)
          ) {
            df.withColumn(
              partitionColumn,
              from_unixtime(col(Constants.CREATED_AT).cast(IntegerType) + lit(19800), "yyyy-MM-dd")
            )
          } else { df }
        case _ => df
      }

      partitionedDf.write
        .mode(SaveMode.Overwrite)
        .partitionBy(partitionColumn)
        .saveAsTable(outputTable)
    }
  }

  /**
   * Runs transform against dataframe read from jdbc and writes it to Delta table
   */
  def run(): Unit = {
    val df = if (importConfig.mapColumns.nonEmpty) {
      DataTransforms.castColumns(sourceDataframe, importConfig.mapColumns.get)
    } else { sourceDataframe }

    importConfig.partitionBy match {
      case None                  => df.writeToParquet(importConfig.outputTable)
      case Some(partitionColumn) => df.writeAsPartitioned(importConfig.outputTable, partitionColumn)
    }
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
