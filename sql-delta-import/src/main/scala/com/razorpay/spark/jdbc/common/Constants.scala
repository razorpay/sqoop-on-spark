package com.razorpay.spark.jdbc.common

import scala.io.Source

object Constants {
  final val HUDI_DB_PREFIX = "realtime_"

  final val MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
  final val POSTGRESQL_DRIVER = "org.postgresql.Driver"

  final val MYSQL = "mysql"
  final val POSTGRESQL = "postgresql"

  // seconds
  final val QUERY_TIMEOUT = 10800

  val envFilePath = "/opt/env"
  val env = Source.fromFile(envFilePath).getLines().mkString

  final val CREDSTASH_TABLE_NAME = s"credstash-$env-emr-sqoop"

  final val CREATED_DATE = "created_date"
  final val CREATED_AT = "created_at"

  final val COLUMN_DATATYPE_MAPPING = Map(
    "bigint" -> "long",
    "boolean" -> "boolean"
  )

  final val STRING = "string"
  final val DECIMAL_TYPE = "DecimalType"

  val dataTypeMapping = Map(
    "StringType"    -> "string",
    "LongType"      -> "bigint",
    "IntegerType"   -> "bigint",
    "ShortType"     -> "smallint",
    "ByteType"      -> "tinyint",
    "DoubleType"    -> "double",
    "BooleanType"   -> "boolean",
    "TimestampType" -> "timestamp"
  )
}
