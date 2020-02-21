package com.company

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object CountDistinct extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("value", StringType) :: Nil)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = (buffer.getSeq[String](0).toSet + input.getString(0)).toSeq
  }

  override def bufferSchema: StructType = StructType(
    StructField("items", ArrayType(StringType, true)) :: Nil
  )

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = (buffer1.getSeq[String](0).toSet ++ buffer2.getSeq[String](0).toSet).toSeq
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq[String]()
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    buffer.getSeq[String](0).length
  }

  override def dataType: DataType = IntegerType
}