package com.databricks

import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class TaskInfoMetrics(stageId: Int,
                           stageAttemptId: Int,
                           taskType: String,
                           index: Long,
                           taskId: Long,
                           attemptNumber: Int,
                           launchTime: Long,
                           finishTime: Long,
                           duration: Long,
                           schedulerDelay: Long,
                           executorId: String,
                           host: String,
                           taskLocality: String,
                           speculative: Boolean,
                           gettingResultTime: Long,
                           successful: Boolean,
                           executorRunTime: Long,
                           executorCpuTime: Long,
                           executorDeserializeTime: Long,
                           executorDeserializeCpuTime: Long,
                           resultSerializationTime: Long,
                           jvmGCTime: Long,
                           resultSize: Long,
                           numUpdatedBlockStatuses: Int,
                           diskBytesSpilled: Long,
                           memoryBytesSpilled: Long,
                           peakExecutionMemory: Long,
                           recordsRead: Long,
                           bytesRead: Long,
                           recordsWritten: Long,
                           bytesWritten: Long,
                           shuffleFetchWaitTime: Long,
                           shuffleTotalBytesRead: Long,
                           shuffleTotalBlocksFetched: Long,
                           shuffleLocalBlocksFetched: Long,
                           shuffleRemoteBlocksFetched: Long,
                           shuffleWriteTime: Long,
                           shuffleBytesWritten: Long,
                           shuffleRecordsWritten: Long,
                           errorMessage: Option[String])

class TaskMetricsExplorer(sparkSession: SparkSession) {
  val listenerTask = new TaskInfoRecorderListener()
  sparkSession.sparkContext.addSparkListener(listenerTask)

  private def measureTime[A](f: => A): Double = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    return (endTime - startTime).toDouble/1000000000
  }

  def runAndMeasure[T](f: => T): DataFrame = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    println(s"Time taken: ${(endTime - startTime) / 1000000} ms")
    createDF(listenerTask.taskInfoMetrics)
  }

  def runQueryStringAndMeasure(query:String): (DataFrame,Map[String, Double], DataFrame, Array[Row]) = {
    val df = spark.sql(query)
    val queryExecution = df.queryExecution
    val parsingTime = measureTime{
      queryExecution.logical
    }
    println(parsingTime)
    val analysisTime = measureTime {
      queryExecution.analyzed
    }
    println(analysisTime)
    val optimizationTime = measureTime {
      queryExecution.optimizedPlan
    }
    println(optimizationTime)
    val planningTime = measureTime {
      queryExecution.executedPlan
    }
    println(planningTime)
    val executionStartTime = System.nanoTime().toDouble
    val result = spark.sql(query).collect()
    val executionEndTime = System.nanoTime().toDouble
    val executionTime = (executionEndTime - executionStartTime)/1000000000
    println(executionTime)
    return (
      createDF(listenerTask.taskInfoMetrics), 
      Map(
        "parsingTime" -> parsingTime,
        "analysisTime" -> analysisTime,
        "optimizationTime" -> optimizationTime,
        "planningTime" -> planningTime,
        "executionTime" -> executionTime,
        "runTime" -> (parsingTime + analysisTime + optimizationTime + planningTime + executionTime)
      ),
      df,
      result
    )
  }

  private def createDF(taskEnd: mutable.Buffer[(Int, Int, String, TaskInfo, TaskMetrics, TaskEndReason)]): DataFrame = {
    import sparkSession.implicits._
    lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
    val row = taskEnd.map { case (stageId, stageAttemptId, taskType, taskInfo, taskMetrics, taskEndReason) => 
      val errorMessage = taskEndReason match {
        case Success =>
          Some("Success")
        case k: TaskKilled =>
          Some(k.reason)
        case e: ExceptionFailure =>
          Some(e.toErrorString)
        case e: TaskFailedReason =>
          Some(e.toErrorString)
        case other =>
          logger.info(s"Unhandled task end reason: $other")
          None
      }
      
      val gettingResultTime = {
        if (taskInfo.gettingResultTime == 0L) 0L
        else taskInfo.finishTime - taskInfo.gettingResultTime
      }
      
      val schedulerDelay = math.max(0L, taskInfo.duration - taskMetrics.executorRunTime - taskMetrics.executorDeserializeTime -
        taskMetrics.resultSerializationTime - gettingResultTime)
      
      TaskInfoMetrics(
        stageId,
        stageAttemptId,
        taskType,
        taskInfo.index,
        taskInfo.taskId,
        taskInfo.attemptNumber,
        taskInfo.launchTime,
        taskInfo.finishTime,
        taskInfo.duration,
        schedulerDelay,
        taskInfo.executorId,
        taskInfo.host,
        taskInfo.taskLocality.toString,
        taskInfo.speculative,
        gettingResultTime,
        taskInfo.successful,
        taskMetrics.executorRunTime,
        taskMetrics.executorCpuTime / 1000000,
        taskMetrics.executorDeserializeTime,
        taskMetrics.executorDeserializeCpuTime / 1000000,
        taskMetrics.resultSerializationTime,
        taskMetrics.jvmGCTime,
        taskMetrics.resultSize,
        taskMetrics.updatedBlockStatuses.length,
        taskMetrics.diskBytesSpilled,
        taskMetrics.memoryBytesSpilled,
        taskMetrics.peakExecutionMemory,
        taskMetrics.inputMetrics.recordsRead,
        taskMetrics.inputMetrics.bytesRead,
        taskMetrics.outputMetrics.recordsWritten,
        taskMetrics.outputMetrics.bytesWritten,
        taskMetrics.shuffleReadMetrics.fetchWaitTime,
        taskMetrics.shuffleReadMetrics.totalBytesRead,
        taskMetrics.shuffleReadMetrics.totalBlocksFetched,
        taskMetrics.shuffleReadMetrics.localBlocksFetched,
        taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
        taskMetrics.shuffleWriteMetrics.writeTime / 1000000,
        taskMetrics.shuffleWriteMetrics.bytesWritten,
        taskMetrics.shuffleWriteMetrics.recordsWritten,
        errorMessage
      ) 
    }
    row.toDF()
  }
}
