package com.utils

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object SchemaUtils2 {
  val structtype = StructType(
    Seq(
      StructField("provincename",StringType),
      StructField("cityname",StringType),
      StructField("origin",StringType),
      StructField("valid",StringType),
      StructField("ad",StringType),
      StructField("joiner",StringType),
      StructField("suc",StringType),
      StructField("shower",StringType),
      StructField("cli",StringType),
      StructField("DSPcon",StringType),
      StructField("DSPcos",StringType)
    )
  )

}
