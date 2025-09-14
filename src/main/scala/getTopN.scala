import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{DataType, StructType}
import scala.io.Source
import java.io.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.json4s._
import org.json4s.jackson.JsonMethods._

object getTopN {

  def main(args: Array[String]): Unit = {

    // read yaml config
    val yamlMapper = new ObjectMapper(new YAMLFactory())
    val config = yamlMapper.readTree(new File("./config.yaml"))
    val INPUT_DATA_FILEPATH = config.get("INPUT_DATA_FILEPATH").asText()
    val INPUT_MAPPING_FILEPATH = config.get("INPUT_MAPPING_FILEPATH").asText()
    val OUTPUT_FILEPATH = config.get("OUTPUT_FILEPATH").asText()
    val top_N = config.get("top_N").asInt()

    // read json schema
    val schemaSource = Source.fromFile("schema.json").mkString
    val json = parse(schemaSource)

    // extract individual schemas as compact JSON strings
    val videoSchemaJson = compact(render(json \ "video_data"))
    val mappingSchemaJson = compact(render(json \ "mapping_table"))
    val outputSchemaJson = compact(render(json \ "output_data"))

    // convert to StructType
    val videoSchema = DataType.fromJson(videoSchemaJson).asInstanceOf[StructType]
    val mappingSchema = DataType.fromJson(mappingSchemaJson).asInstanceOf[StructType]
    val outputSchema = DataType.fromJson(outputSchemaJson).asInstanceOf[StructType]

    val spark = SparkSession.builder()
      .appName("htx")
      .master("local[*]") // run locally
      .getOrCreate()

    // read dataframe with dataframe api
//    val videoDF = spark.read.schema(videoSchema).parquet(INPUT_DATA_FILEPATH)
//    val mappingDF = spark.read.schema(mappingSchema).parquet(INPUT_MAPPING_FILEPATH)
    val videoDF = spark.read.parquet(INPUT_DATA_FILEPATH)
    val mappingDF = spark.read.parquet(INPUT_MAPPING_FILEPATH)


    // load spark context
    val sc = spark.sparkContext

    // transform into rdd
    var videoRDD = videoDF.rdd
    val mappingRDD = mappingDF.rdd

    // deduplication due to possibility of upstream ingestion issue
    videoRDD = videoRDD.distinct()

    // groupby and count
    // with key being geographical_location_oid
    // group by key, sort, and enumerate
    val kvRDD = videoRDD.map(row => ((row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name")), 1))
    val aggRDD = kvRDD.reduceByKey(_ + _)
    val grpRDD = aggRDD.map { case ((geoId, itemName), count) => (geoId, (itemName, count)) }
      .groupByKey()
      .mapValues(iter => iter.toList.sortBy(-_._2).zipWithIndex.map { case ((itemName, count), i) => (i.toLong + 1, itemName, count) })

    // filter by top N configured by yaml
    val topNRDD = grpRDD.flatMap { case (geoId, list) =>
      list.map { case (rank, itemName, count) => (geoId, rank, itemName, count) }
    }.filter(_._2 <= top_N)

    // perform broadcast join via mapping
    val mappingDict = mappingRDD.collect().map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location"))).toMap
    val mappingBroadcast = sc.broadcast(mappingDict)

    // manually join using broadcast variable
    val joinedRDD = topNRDD.map { case (geoId, rank, itemName, count) =>
      (mappingBroadcast.value(geoId), rank, itemName)
    }

    // create a spark dataframe to be written out
    val outputDF = spark.createDataFrame(joinedRDD.map(Row.fromTuple), outputSchema)

    outputDF.show(20)

    outputDF.repartition(1)
      .write
      .mode("append")
      .parquet(OUTPUT_FILEPATH)

    spark.stop()
  }
}
