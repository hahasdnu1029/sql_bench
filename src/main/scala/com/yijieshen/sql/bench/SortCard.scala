package com.yijieshen.sql.bench

import org.apache.spark.sql.catalyst.InternalRow

import scala.sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.util.Random

case class SortCardConfig(end: Long = 0, p: Int = 0, fc: Int = 0, sc: Int = 0, cn: Int = 0, mode: String = "EXEC")

object SortCard {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[SortCardConfig](SortBench.getClass.getCanonicalName.stripSuffix("$")) {
      head("Sort Operator Benchmark")
      opt[Long]('e', "end")
        .action((x, c) => c.copy(end = x))
        .text("num of items to sort")
      opt[Int]('p', "partition")
        .action((x, c) => c.copy(p = x))
        .text("num of tasks to sort")
      opt[Int]('f', "fsc")
        .action((x, c) => c.copy(fc = x))
        .text("first sort key cardinality")
      opt[Int]('s', "ssc")
        .action((x, c) => c.copy(sc = x))
        .text("second sort key cardinality")
      opt[Int]('n', "columnNum")
        .action((x, c) => c.copy(cn = x))
        .text("num of non sort by columns")
      opt[String]('m', "mode")
        .action((x, c) => c.copy(mode = x.toUpperCase))
        .validate(x => if (x.toUpperCase.equals("EXEC") || x.toUpperCase.equals("EXP")) {
          success
        } else {
          failure("Option --mode must be either 'EXEC'(execution) or 'EXP'(explain)")
        })
        .text("execution mode: EXEC for execution and EXP for explain")
      help("help")
        .text("print this usage text")
    }
    parser.parse(args, SortCardConfig()) match {
      case Some(config) => run(config)
      case None => System.exit(1)
    }

  }

  protected def measureTimeMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }

  def run(config: SortCardConfig): Unit = {
    val conf = new SparkConf().setAppName("SortCard")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val sortColumns = randInt(Random.nextInt(), config.fc).as("s_0") :: randInt(Random.nextInt(), config.sc).as("s_1") :: Nil
    val otherColumns = (0 until config.cn).map(i => $"id".as(s"o_$i"))

    val result = sqlContext.range(0, config.end, 1, config.p)
      .select(sortColumns ++ otherColumns: _*)
      .sortWithinPartitions("s_0", "s_1")

//    val columnStr = result.schema.map(_.name).mkString(",")
//
//    val xxx = result.selectExpr(s"hash($columnStr) as hashValue")
//      .groupBy()
//      .sum("hashValue")

    if (config.mode.equals("EXP")) {
      result.explain(true)
    } else {
      if (new java.io.File("/home/syj/free_memory.sh").exists) {
        val commands = Seq("bash", "-c", s"/home/syj/free_memory.sh")
        commands.!!
        System.err.println("free_memory succeed")
      } else {
        System.err.println("free_memory script doesn't exists")
      }



      val time = measureTimeMs {
        val plan = result.queryExecution.executedPlan
        if (plan.outputsRowBatches) {
          plan.batchExecute().foreach(b => Unit)
        } else {
          plan.execute().foreachPartition { case x: Iterator[InternalRow] =>
            var sum: Long = 0
            while (x.hasNext) {
              sum += x.next().hashCode()
            }
            println(sum)
          }
        }
      }
      println(s"Sort takes ${time / 1000}s to finish.")
    }
    sc.stop()
  }
}
