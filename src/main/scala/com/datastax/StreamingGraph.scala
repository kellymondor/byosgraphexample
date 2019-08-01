package com.datastax

import org.apache.spark.sql.SparkSession
import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StreamingGraph {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Streaming Graph Load Application")
      .config("spark.cleaner.ttl", "3600")
      .config("spark.streaming.stopGracefullyOnShutdown","false")
      .getOrCreate()

    def customerSchema(): StructType = {
      StructType(Array(
        StructField("customer_id", StringType, false),
        StructField("company_name", StringType, true),
        StructField("contact_name", StringType, true),
        StructField("contact_title", StringType, true),
        StructField("address", StringType, true),
        StructField("city", StringType, true),
        StructField("region", StringType, true),
        StructField("postal_code", StringType, true),
        StructField("country", StringType, true),
        StructField("number", StringType, true),
        StructField("fax", StringType, true)))
    }

    val g = spark.dseGraph("northwind")

    spark
      .readStream
      .option("sep", ",")
      .schema(customerSchema)
      .csv("dsefs:///northwind")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        batchDF.persist()

        g.updateVertices("customer",
          batchDF.select(
            col("customer_id"),
            col("company_name"),
            col("contact_name"),
            col("contact_title")
          )
        )

        g.updateVertices("contact_number",
          batchDF
            .select("number")
        )

        g.updateEdges("customer", "contact_at", "contact_number",
          batchDF.select(
            col("customer_id") as "customer_customer_id",
            col("contact_name") as "customer_company_name",
            col("number") as "contact_number_number"
          )
        )

        batchDF.unpersist()

      }
      .outputMode("update")
      .start()

    spark.streams.awaitAnyTermination()

  }
}
