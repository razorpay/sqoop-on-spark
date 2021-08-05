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

import com.razorpay.spark.jdbc.config.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Spark app that wraps functionality of JDBCImport and exposes configuration as command line args
 */
object ImportRunner extends App {

  val config = new ImportRunnerConfig(args)
  val appConf = ConfigLoader.load()

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName(appConf.getString("app.name"))
    .getOrCreate()

  val importConfig = ImportConfig(
    config.inputTable(),
    config.query.toOption,
    config.boundaryQuery.toOption,
    config.outputTable(),
    config.splitBy.toOption,
    config.chunks(),
    config.partitionBy.toOption,
    config.database(),
    config.mapColumns.toOption
  )

  JDBCImport(
    scope = config.scope(),
    importConfig = importConfig
  ).run()
}

class ImportRunnerConfig(arguments: Seq[String]) extends ScallopConf(arguments) {
  override def mainOptions: Seq[String] =
    Seq("scope", "inputTable", "outputTable", "splitBy", "database")

  val scope: ScallopOption[String] = opt[String](required = true)
  val database: ScallopOption[String] = opt[String](required = true)
  val inputTable: ScallopOption[String] = opt[String](required = true)
  val query: ScallopOption[String] = opt[String](required = false)
  val boundaryQuery: ScallopOption[String] = opt[String](required = false)
  val outputTable: ScallopOption[String] = opt[String](required = true)
  val splitBy: ScallopOption[String] = opt[String](required = false)
  val chunks: ScallopOption[Int] = opt[Int](default = Some(1))
  val partitionBy: ScallopOption[String] = opt[String](required = false)
  val mapColumns: ScallopOption[String] = opt[String](required = false)

  verify()
}
