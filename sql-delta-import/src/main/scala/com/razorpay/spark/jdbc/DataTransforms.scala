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
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

/**
 * Class that applies transformation functions one by one on input DataFrame
 */
object DataTransforms extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def castColumns(df: DataFrame, columnMapping: String): DataFrame = {
    val columnList = columnMapping
      .split(",")
      .map(x => {
        val schema = x.split("=")

        (schema(0).trim, schema(1).trim)
      })

    var castedDf = df

    columnList.foreach(x => {
      if (df.columns.contains(x._1)) {
        castedDf = castedDf
          .withColumn(
            x._1,
            df.col(x._1)
              .cast(Constants.COLUMN_DATATYPE_MAPPING.getOrElse(x._2, Constants.STRING))
          )
      } else {
        // todo: fix this, logger.error results in nullPointerException
        println(s"`${x._1}` column does not exist in the table, skipping")
      }
    })
    castedDf
  }

}
