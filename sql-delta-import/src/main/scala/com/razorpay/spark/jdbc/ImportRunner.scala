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

import com.razorpay.spark.jdbc.common.Constants
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Spark app that wraps functionality of JDBCImport and exposes configuration as command line args
 */
object ImportRunner extends App {

  val config = new ImportRunnerConfig(args)

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-snapshot-backfill")
    .enableHiveSupport()
    .getOrCreate()

  val importConfig = ImportConfig(
    config.s3ReadPath(),
    config.outputTable(),
    config.partitionBy.toOption,
    config.mapColumns.toOption,
    config.s3WriteBucket.toOption,
    config.maxExecTimeout(),
    config.schema.toOption
  )

  S3Import(
    importConfig = importConfig
  ).run()
}

class ImportRunnerConfig(arguments: Seq[String]) extends ScallopConf(arguments) {

  override def mainOptions: Seq[String] =
    Seq("s3ReadPath", "outputTable")

  val s3ReadPath: ScallopOption[String] = opt[String](required = true)
  val outputTable: ScallopOption[String] = opt[String](required = true)
  val partitionBy: ScallopOption[String] = opt[String](required = false)
  val mapColumns: ScallopOption[String] = opt[String](required = false)
  val s3WriteBucket: ScallopOption[String] = opt[String](required = false)

  val maxExecTimeout: ScallopOption[Long] =
    opt[Long](default = Some(Constants.QUERY_TIMEOUT * 1000L))
  val schema: ScallopOption[String] = opt[String](required = false)

  verify()
}
