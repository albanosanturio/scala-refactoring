import org.apache.spark.sql.{DataFrame, Row}
//val df = spark.read.option("header","true").csv("abfss://pull-raw-data@dlsypnaseiedrnysegrgeint.dfs.core.windows.net/Poc-AsstMgmt/output.csv")


def create_external_table(database: String, tableName: String, inputFilePath: String): Unit = {
  val df = spark.read.option("header","true").csv(inputFilePath)

  val columns = df.schema.fields.map {
  field => field.name + " " + field.dataType.typeName
  }.mkString(", ")
  spark.sql("CREATE DATABASE IF NOT EXISTS "+database)
  //Create temp view
  df.createOrReplaceTempView("myPreOutputTable")

  //Create the table if not existing
  if (!spark.catalog.tableExists(database + "." + tableName)) {
            spark.sql("CREATE EXTERNAL TABLE " + database + "." + tableName + " (" + columns +
              """)
                |PARTITIONED BY (year SMALLINT, month SMALLINT)
                |STORED AS PARQUET
                |LOCATION """.stripMargin + " '/"  + database + ".db/" + tableName + "'")
    }

          //Write the data in the table
          spark.sql(
            s"""INSERT OVERWRITE TABLE $database.$tableName PARTITION (year = 2022, month = 09)
              |SELECT * FROM myPreOutputTable
              |""".stripMargin
          )

  //Drop temp view
  spark.catalog.dropTempView("myPreOutputtable")
}

//create_external_table('asstsmgmt', '')

// load function 
/*
    Each of the load functions follows the same high level logic: 
    1. Extract the latest partition of the input table(s), coming from lower level tables/raw zone tables
    2. Transform the extracted data according to business rules
    3. Load the results into the output tables 
*/

// Encapsulate table info
trait PartitionSchema 
case class YearMonth() extends PartitionSchema
case class ExecutionDate() extends PartitionSchema
case class Table(tableName: String, partitionSchema: PartitionSchema)

// type alias for the Transformation part of each process
type Transformation = PartialFunction[List[DataFrame], DataFrame]

// Abstraction to check latest partition and load data
def getLatestPartition(dbName: String)(table: Table): (DataFrame, Option[(String, String)]) = {

    // get all partitions (this solution avoids scanning the whole table and only queries the metadata for most recent partition)
    val partitions: DataFrame = spark.sql(s"SHOW PARTITIONS ${table.tableName}")

    val df = spark.table(s"${dbName}.${table.tableName}")

    table.partitionSchema match {
        case pSchema: YearMonth => {
            val fileDateRegex: String = """FILE_YEAR=(2[0-9]{3})\/FILE_MONTH=([01][0-9])"""

            // extract year and month from partition string
            val partitionYearMonth: DataFrame = partitions.withColumn("year",
            regexp_extract(upper(col("partition")), fileDateRegex, 1).cast("string"))
            .withColumn("month", regexp_extract(upper(col("partition")), fileDateRegex, 2).cast("string"))

            // order by file year, file month and select the first one (this gives most recent partition)
            val mostRecentPartition: Row = partitionYearMonth.select("year", "month")
            .orderBy(desc("year"), desc("month")).first

            val maxYear = mostRecentPartition.getString(0)
            val maxMonth = if (mostRecentPartition.getString(1).toInt >= 10) mostRecentPartition.getString(1) else {
                s"0${mostRecentPartition.getString(1)}"
            }

            (df.where(s"FILE_YEAR = ${maxYear} AND FILE_MONTH = ${maxMonth}"), Some((maxYear, maxMonth)))
        } 
        case pSchema: ExecutionDate => {
            // in this case, partition is by execution date
            val maxDate: String = partitions.select(max(col("EXECUTION_DATE"))).first.getDate(0).toString

            (df.where(s"EXECUTION_DATE = '${maxDate}'"), None)
        }
    }
}


def writeOutputToTable(dbName: String)(targetTable: Table, outputDF: DataFrame, insertPartition: (String, String)): Unit = {
    outputDF.createOrReplaceTempView("preoutputTable")
    targetTable.partitionSchema match {
        case pSchema: YearMonth => {
            val (maxYear, maxMonth) = insertPartition
            spark.sql(s"""INSERT OVERWRITE TABLE ${dbName}.${targetTable.tableName} PARTITION(FILE_YEAR=$maxYear, FILE_MONTH=$maxMonth) SELECT * FROM preoutputTable""")
        }
        case pSchema: ExecutionDate => {
            val currentDate = java.time.LocalDate.now
            spark.sql(s"""INSERT OVERWRITE TABLE ${dbName}.${targetTable.tableName} PARTITION(EXECUTION_DATE=${currentDate.toString}) SELECT * FROM preoutputTable""")
        }
    } 

    spark.catalog.dropTempView("preoutputTable")
}


// loadTable implies Extract, Transform & load
// Rename loadTable to ETLTable or runAll?
def loadTable(dataStoreName: Option[String], dbName: String)(sourceTables: List[Table], outputTable: Table, transform: Transformation): Unit = (dataStoreName: Option[String], dbName: String) => {
    val latestPartitionGetter: Table => (DataFrame, Option[(String, String)]) = dataStoreName match {
        // load tables from datastore
        case Some(dsName) => getLatestPartition(dsName)
        // load tables from db 
        case None => getLatestPartition(dbName) 
    }

    // we might have multiple input tables (must return file_months and file_years)
    val latestPartitionInputDFs: List[DataFrame] = sourceTables.map(latestPartitionGetter(_)._1)
    // get the maximum file_month, file_year for the partition by file_month, file_year case
    val maximumInputPartitions: (String, String) = sourceTables.map(latestPartitionGetter(_)._2).map(_.getOrElse(("0", "0"))).reduce {(a, b) => if (a._1.toInt > b._1.toInt) {
      if (a._2.toInt > b._2.toInt) a else b}
      else b}

    // transform the data (TODO: Implement each of the transformations of AAM as anonymous functions)
    val transformedDF: DataFrame = transform(latestPartitionInputDFs)

    // TODO: Load the resulting table into output table (needs to specify partition mode)
    writeOutputToTable(dbName)(outputTable, transformedDF, maximumInputPartitions)
	
	import org.apache.spark.sql.functions._

// load functions go to aslan
// revise intake parts (logic)
// standarize saving df to table

// var flag = false repeats... is it worth creating an object to check_updateds?
// updated_file and updated_partition come from the same codebase but i decided to separate them
// Encapsulate all checkX methods in object that checks if code should execute or not?


/** Returns true if there's a file that contains todays date.
*  Originated from AssetManagement(PreliminaryHealthScore)
*  Original name fileFromToday in PreliminaryHealthScore.scala
*
*  @param spark the SparkSession
*  @param db source database to compare API data
*  @param db2 sink database to compare API data
*  @param history_paths list of paths where to check for new files
*/
def checkUpdatedFile(historyPaths: Array[String]): Boolean =  {
    import org.apache.spark.sql.functions.{current_timestamp, date_format}

    val dateFormat = "yyyy-MM-dd"
    val currentDate = spark.range(1).select(date_format(current_timestamp, dateFormat)).collectAsList().get(0).get(0).toString

    val result = historyPaths.flatMap(x =>  mssparkutils.fs.ls(x)).exists(_.name.contains(currentDate))
    result
}

/**
*  Originated from CircuitBreakers Validation
*  This function validated new files in history path for each source in Raw
*/
def checkUpdatedTable(dataBase: String, dateColumn: String, tables: Array[String]): Boolean = {
    val result = tables.map(x => 
        spark.table(s"$dataBase.$x").select(datediff(current_date(), to_date(max(col("$dateColumn")))))
        .first.getInt(0)).exists(x)
    result
}

/**
*  Originated from AsstMgmt(PreliminaryHealthScore)
*  Returns true if there are more partitions in the rawTable than refinedTable
*  @param rawTable String representing raw db.table location
*  @param refinedTable String representing refined db.table location
*/
case class Tables(rawTable: String, refinedTable: String)
def checkUpdatedPartition(tables: List[Tables]): Boolean = {
    val result = tables.exists(x,y => spark.sql(s"SHOW PARTITIONS $x").count > spark.sql(s"SHOW PARTITIONS $y").count)
    result
}

// Testing zone for the methods
// TODO: test _partition and _table method when we have tables
//val newFile = checkUpdatedFile(Array("abfss://pull-raw-data@dlsypnaseiedrnysegrgeint.dfs.core.windows.net/poc_asstmgmt/",
// "abfss://iedrnysegrgeint@dlsypnaseiedrnysegrgeint.dfs.core.windows.net/"))
