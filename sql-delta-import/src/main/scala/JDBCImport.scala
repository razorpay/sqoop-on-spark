package io.delta.connectors.spark.JDBC

import io.delta.connectors.spark.JDBC.spark._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Class that contains jdbc source, read parallelism params and target table name
 *
 *  @param source       - jdbc source table
 *  @param destination  - Delta target database.table
 *  @param splitBy      - column by which to split source data while reading
 *  @param chunks       - to how many chunks split jdbc source data
 **/
case class ImportConfig(source: String, destination: String, splitBy: String, chunks: Int) {
  val bounds_sql = s"(select min($splitBy), max($splitBy) from $source) as bounds"
}

/**
 * Class that does reading from jdbc source,transform and writing to Delta table
 *
 *  @param jdbcUrl       - url connecting string for jdbc source
 *  @param importConfig  - case class that contains source read parallelism params and target table
 *  @param jdbcParams    - additional jdbc session params like isolation level, perf tuning, net wait params etc...
 *  @param dataTransform - contains function that we should apply to transform our source data
 **/
class JDBCImport(jdbcUrl: String,
                 importConfig: ImportConfig,
                 jdbcParams: Map[String, String] = Map(),
                 dataTransform: DataTransform)
                (implicit val spark: SparkSession) {

  import spark.implicits._

  // list of columns to import is obtained from schema of destination delta table
  private val targetColumns = DeltaTable
    .forName(importConfig.destination)
    .toDF
    .schema
    .fields
    .map(_.name)

  private val sourceDataframe = readJdbcSourceInParallel()
    .select(targetColumns.map(col): _*)

  /**
   * obtains lower and upper bound of source table and uses those values to read in a JDBC dataframe
   * @return a dataframe read from source table
   */
  private def readJdbcSourceInParallel(): DataFrame = {

    val (lower, upper): (Long, Long) = spark
      .read
      .jdbc(jdbcUrl, importConfig.bounds_sql, jdbcParams)
      .as[(Option[Long], Option[Long])]
      .collect()
      .map{ case (a, b) => (a.getOrElse(0L), b.getOrElse(0L))}
      .head

    spark
      .read
      .jdbc(jdbcUrl, importConfig.source, importConfig.splitBy, lower, upper, importConfig.chunks, jdbcParams)
  }

  private implicit class DataFrameExtensionOps(df: DataFrame) {

    def runTransform(): DataFrame = dataTransform.runTransform(sourceDataframe)

    def writeToDelta(deltaTableToWrite: String): Unit = df
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .insertInto(deltaTableToWrite)
  }

  /**
   * Runs transform against dataframe read from jdbc and writes it to Delta table
   **/
  def run(): Unit = {
    sourceDataframe
      .runTransform()
      .writeToDelta(importConfig.destination)
  }

}

object JDBCImport {
  def apply(jdbcUrl: String,
            importConfig: ImportConfig,
            jdbcParams: Map[String, String] = Map(),
            dataTransform: DataTransform)
           (implicit spark: SparkSession): JDBCImport = {

    new JDBCImport(jdbcUrl, importConfig, jdbcParams, dataTransform)
  }

}
