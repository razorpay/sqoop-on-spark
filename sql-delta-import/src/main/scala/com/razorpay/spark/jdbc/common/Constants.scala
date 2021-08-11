package com.razorpay.spark.jdbc.common

object Constants {
  final val SCOPE = "spark_sqoop"

  final val MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
  final val POSTGRESQL_DRIVER = "org.postgresql.Driver"

  // seconds
  final val QUERY_TIMEOUT = 10800

  final val CREATED_DATE = "created_date"
  final val CREATED_AT = "created_at"

  final val COLUMN_DATATYPE_MAPPING = Map(
    "bigint" -> "long"
  )

  final val STRING = "string"
  final val DECIMAL_TYPE = "DecimalType"

  val dataTypeMapping = Map(
      "StringType"  -> "string",
      "LongType"    -> "bigint",
      "IntegerType" -> "bigint",
      "ShortType"   -> "smallint",
      "ByteType"    -> "tinyint",
      "DoubleType"  -> "double",
      "BooleanType" -> "boolean"
    )
}
