package com.yijieshen.sql.bench.tpch

import scala.sys.process._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.sql.types._

// TODO dbgen should output directly to stdout instead of file
class Tables(sqlContext: SQLContext) extends Serializable {
  import sqlContext.implicits._

  var dbgenDir: String = _ // the dbgen tool's dir, should exist on each worker
  var scaleFactor: Int = 0

  lazy val dbgen = s"$dbgenDir/dbgen"

  def sparkContext = sqlContext.sparkContext

  case class Table(name: String, nameAlias: String, parallelGen: Boolean, partitionColumns: Seq[String], fields: StructField*) {
    val schema = StructType(fields)
    val pd = if (parallelGen) 100 else 1  // degree of parallelism

    def nonPartitioned: Table = {
      Table(name, nameAlias, parallelGen, Nil, fields : _*)
    }

    /**
      *  If convertToSchema is true, the data from generator will be parsed into columns and
      *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
      */
    def df(convertToSchema: Boolean) = {
      val generatedData = {
        sparkContext.parallelize(1 to pd, pd).flatMap { i =>
          val localToolsDir = if (new java.io.File(dbgen).exists) {
            dbgenDir
          } else if (new java.io.File(s"/$dbgen").exists) {
            s"/$dbgenDir"
          } else {
            sys.error(s"Could not find dbgen at $dbgen or /$dbgen. Run install")
          }

          val parallel = if (pd > 1) s"-C $pd -S $i" else ""
          val fileName = if (pd > 1) s"$name.tbl.$i" else s"$name.tbl"
          val commands = Seq(
            "bash", "-c",
            s"cd $localToolsDir && ./dbgen -f -T $nameAlias -s $scaleFactor $parallel && cat $fileName")
          println(commands)
          commands.lines
        }
      }

      generatedData.setName(s"$name, sf=$scaleFactor, strings")

      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def useDoubleForDecimal(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, nameAlias, parallelGen, partitionColumns, newFields:_*)
    }

    def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text")
      val tempTableName = s"${name}_text"
      data.registerTempTable(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // If the table is not partitioned, coalesce the data to a single file.
        data.write
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      println(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createTemporaryTable(location: String, tableName:String,format: String): Unit = {
      if (format.equals("text")) {
        if (tableName.equals("lineitem")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toLong, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim))
          var schema = StructType(
            List(
              StructField("l_orderkey", LongType, true),
              StructField("l_partkey", IntegerType, true),
              StructField("l_suppkey", IntegerType, true),
              StructField("l_linenumber", IntegerType, true),
              StructField("l_quantity", DoubleType, true),
              StructField("l_extendedprice", DoubleType, true),
              StructField("l_discount", DoubleType, true),
              StructField("l_tax", DoubleType, true),
              StructField("l_returnflag", StringType, true),
              StructField("l_linestatus", StringType, true),
              StructField("l_shipdate", StringType, true),
              StructField("l_commitdate", StringType, true),
              StructField("l_receiptdate", StringType, true),
              StructField("l_shipinstruct", StringType, true),
              StructField("l_shipmode", StringType, true),
              StructField("l_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
        if (tableName.equals("orders")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toLong, p(1).toInt, p(2).trim, p(3).toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).toInt, p(8).trim))
          var schema = StructType(
            List(

              StructField("o_orderkey", LongType, true),
              StructField("o_custkey", IntegerType, true),
              StructField("o_orderstatus", StringType, true),
              StructField("o_totalprice", DoubleType, true),
              StructField("o_orderdate", StringType, true),
              StructField("o_orderpriority", StringType, true),
              StructField("o_clerk", StringType, true),
              StructField("o_shippriority", IntegerType, true),
              StructField("o_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
        if (tableName.equals("partsupp")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toDouble, p(4).trim))
          var schema = StructType(
            List(
              StructField("ps_partkey", IntegerType, true),
              StructField("ps_suppkey", IntegerType, true),
              StructField("ps_availqty", IntegerType, true),
              StructField("ps_supplycost", DoubleType, true),
              StructField("ps_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
        if (tableName.equals("customer")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt, p(4).trim, p(5).toDouble, p(6).trim, p(7).trim))
          var schema = StructType(
            List(
              StructField("c_custkey", IntegerType, true),
              StructField("c_name", StringType, true),
              StructField("c_address", StringType, true),
              StructField("c_nationkey", IntegerType, true),
              StructField("c_phone", StringType, true),
              StructField("c_acctbal", DoubleType, true),
              StructField("c_mktsegment", StringType, true),
              StructField("c_comment", StringType, true)

            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
        if (tableName.equals("part")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).toInt, p(6).trim, p(7).toDouble, p(8).trim))
          var schema = StructType(
            List(
              StructField("p_partkey", IntegerType, true),
              StructField("p_name", StringType, true),
              StructField("p_mfgr", StringType, true),
              StructField("p_brand", StringType, true),
              StructField("p_type", StringType, true),
              StructField("p_size", IntegerType, true),
              StructField("p_container", StringType, true),
              StructField("p_retailprice", DoubleType, true),
              StructField("p_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
        if (tableName.equals("supplier")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt, p(4).trim, p(5).toDouble, p(6).trim))
          var schema = StructType(
            List(
              StructField("s_suppkey", IntegerType, true),
              StructField("s_name", StringType, true),
              StructField("s_address", StringType, true),
              StructField("s_nationkey", IntegerType, true),
              StructField("s_phone", StringType, true),
              StructField("s_acctbal", DoubleType, true),
              StructField("s_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
        if (tableName.equals("nation")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt, p(3).trim))
          var schema = StructType(
            List(
              StructField("n_nationkey", IntegerType, true),
              StructField("n_name", StringType, true),
              StructField("n_regionkey", IntegerType, true),
              StructField("n_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)

        }
        if (tableName.equals("region")) {
          var rdd = sparkContext.textFile(location).map(x => x.split('|'))
          rdd.map(println(_))
          var rowRDD = rdd.map(p => Row(p(0).toInt, p(1).trim, p(2).trim))
          var schema = StructType(
            List(
              StructField("r_regionkey", IntegerType, true),
              StructField("r_name", StringType, true),
              StructField("r_comment", StringType, true)
            )
          )
          sqlContext.createDataFrame(rowRDD, schema).registerTempTable(name)
        }
      } else {
        sqlContext.read.format(format).load(location).registerTempTable(name)
      }
    }
  }

  def genData(
      dbgenDir: String,
      scaleFactor: Int,
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      useDoubleForDecimal: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: String = ""): Unit = {
    this.dbgenDir = dbgenDir
    this.scaleFactor = scaleFactor

    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    val withSpecifiedDataType = if (useDoubleForDecimal) {
      tablesToBeGenerated.map(_.useDoubleForDecimal())
    } else {
      tablesToBeGenerated
    }

    withSpecifiedDataType.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues)
    }
  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.createTemporaryTable(tableLocation, table.name,format)
    }
  }

  // TODO partition by which col?
  // TODO date type instead of string?
  val tables = Seq(
    Table("lineitem", "L", true,
      partitionColumns = Nil,
      'l_orderkey             .long,
      'l_partkey              .int,
      'l_suppkey              .int,
      'l_linenumber           .int,
      'l_quantity             .double,
      'l_extendedprice        .double,
      'l_discount             .double,
      'l_tax                  .double,
      'l_returnflag           .string,
      'l_linestatus           .string,
      'l_shipdate             .string,
      'l_commitdate           .string,
      'l_receiptdate          .string,
      'l_shipinstruct         .string,
      'l_shipmode             .string,
      'l_comment              .string),
    Table("orders", "O", true,
      partitionColumns = Nil,
      'o_orderkey             .long,
      'o_custkey              .int,
      'o_orderstatus          .string,
      'o_totalprice           .double,
      'o_orderdate            .string,
      'o_orderpriority        .string,
      'o_clerk                .string,
      'o_shippriority         .int,
      'o_comment              .string),
    Table("partsupp", "S", true,
      partitionColumns = Nil,
      'ps_partkey             .int,
      'ps_suppkey             .int,
      'ps_availqty            .int,
      'ps_supplycost          .double,
      'ps_comment             .string),
    Table("customer", "c", true,
      partitionColumns = Nil,
      'c_custkey              .int,
      'c_name                 .string,
      'c_address              .string,
      'c_nationkey            .int,
      'c_phone                .string,
      'c_acctbal              .double,
      'c_mktsegment           .string,
      'c_comment              .string),
    Table("part", "P", true,
      partitionColumns = Nil,
      'p_partkey              .int,
      'p_name                 .string,
      'p_mfgr                 .string,
      'p_brand                .string,
      'p_type                 .string,
      'p_size                 .int,
      'p_container            .string,
      'p_retailprice          .double,
      'p_comment              .string),
    Table("supplier", "s", true,
      partitionColumns = Nil,
      's_suppkey              .int,
      's_name                 .string,
      's_address              .string,
      's_nationkey            .int,
      's_phone                .string,
      's_acctbal              .double,
      's_comment              .string),
    Table("nation", "n", false,
      partitionColumns = Nil,
      'n_nationkey            .int,
      'n_name                 .string,
      'n_regionkey            .int,
      'n_comment              .string),
    Table("region", "r", false,
      partitionColumns = Nil,
      'r_regionkey            .int,
      'r_name                 .string,
      'r_comment              .string)
  )
}
