  package com.avangrid

  import com.typesafe.config.ConfigFactory
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  object CriticalityScore {

  def main(args: Array[String]): Unit = {
    val configFilePath = args(0)
    val config = ConfigFactory.load(configFilePath).getConfig("general")
    val dbs = config.getConfig("dbs")
    val db_name = dbs.getString("am_db")
    val datastore_name = dbs.getString("datastore_db")

    // val db_name = "am_dev"
    // val datastore_name = "avangrid_datastore_dev"
    load_dim_customer_count_l1(spark, db_name, datastore_name)
    load_dim_load_serving_regulatory_compliance_l1(spark, db_name, datastore_name)
    load_fact_load_serving_regulatory_compliance_l2(spark, db_name)
    load_fact_customer_count_l2(spark, db_name)
    load_fact_quality_of_supply_l3(spark, db_name)
    load_fact_criticalityscore_l4(spark, db_name)
    spark.stop()
  }
  }

  val dimCustomerCountL1: Transformation = {
    case customerCount :: Nil => {
      customerCount.select(
        col("SUBSTATION"),
        col("EQUIP"),
        col("ASSET_DX"),
        col("PRIMARY_VOLTAGE"),
        col("SECONDARY_VOLTAGE"),
        col("SUM_OF_CUSTOMERS"))
     }
  }

  val dimLoadServingRegulatoryComplianceL1: Transformation = {
    case loadServingRegulatoryComplianceL1 :: Nil => {
      loadServingRegulatoryComplianceL1.select(
        col("EQUIP"),
	      col("SERIAL_NUM"),
	      col("SUBST"),
	      col("BANK_DX"),
	      col("HIGH_SIDE_VOLTAGE"),
	      col("BASE_NAMEPLATE_KVA"),
	      col("CONST_YR"),
	      col("LOW_LOAD_SERVING"),
	      col("MEDIUM_LOAD_SERVING"),
	      col("HIGH_LOAD_SERVING"),
	      col("VERY_HIGH_LOAD_SERVING"),
	      col("LOW_REGULATORY_COMPLIANCE"),
	      col("MEDIUM_REGULATORY_COMPLIANCE"),
	      col("HIGH_REGULATORY_COMPLIANCE"),
	      col("VERY_HIGH_REGULATORY_COMPLIANCE"))
     }
  }

  val factLoadServingRegulatoryComplianceL2: Transformation = {
    case loadServingRegulatoryComplianceL1 :: dimCharacteristicsL1 :: Nil => {
      val  regulatoryTempFinal100 =loadServingRegulatoryComplianceL1.select(
        col("EQUIP"),
        col("SERIAL_NUM"),
        col("SUBST"),
        col("BANK_DX"),
        col("HIGH_SIDE_VOLTAGE"),
        col("BASE_NAMEPLATE_KVA"),
        col("CONST_YR"),
        col("LOW_LOAD_SERVING"),
        col("MEDIUM_LOAD_SERVING"),
        col("HIGH_LOAD_SERVING"),
        col("VERY_HIGH_LOAD_SERVING"),
        col("LOW_REGULATORY_COMPLIANCE"),
        col("MEDIUM_REGULATORY_COMPLIANCE"),
        col("HIGH_REGULATORY_COMPLIANCE"),
        col("VERY_HIGH_REGULATORY_COMPLIANCE"),
        col("file_year"),
        col("file_month"))

      val regulatoryTempFinal101 = regulatoryTempFinal100
        .withColumn("LOAD_SERVING_CAPABILITY_SCORE", when($"LOW_LOAD_SERVING" === 1, 1)
        .when($"MEDIUM_LOAD_SERVING" === 2, 2)
        .when($"HIGH_LOAD_SERVING" === 3, 3)
        .when($"VERY_HIGH_LOAD_SERVING" === 4, 4)
        .otherwise(1))
        .withColumn("REGULATORY_COMPLIANCE_SCORE", when($"LOW_REGULATORY_COMPLIANCE" === 1, 1)
        .when($"MEDIUM_REGULATORY_COMPLIANCE" === 2, 2)
        .when($"HIGH_REGULATORY_COMPLIANCE" === 3, 3)
        .when($"VERY_HIGH_REGULATORY_COMPLIANCE" === 4, 4)
        .otherwise(1))
  
      val regulatoryTempFinal102 = regulatoryTempFinal101
      .join(dimCharacteristicsL1, Seq("EQUIP"), "right")
      .select(
        dimCharacteristicsL1("EQUIP"),
	      regulatoryTempFinal101("SERIAL_NUM"),
	      regulatoryTempFinal101("SUBST"),
	      regulatoryTempFinal101("BANK_DX"),
	      regulatoryTempFinal101("HIGH_SIDE_VOLTAGE"),
	      regulatoryTempFinal101("BASE_NAMEPLATE_KVA"),
	      regulatoryTempFinal101("CONST_YR"),
	      regulatoryTempFinal101("LOW_LOAD_SERVING"),
	      regulatoryTempFinal101("MEDIUM_LOAD_SERVING"),
	      regulatoryTempFinal101("HIGH_LOAD_SERVING"),
	      regulatoryTempFinal101("VERY_HIGH_LOAD_SERVING"),
	      regulatoryTempFinal101("LOW_REGULATORY_COMPLIANCE"),
	      regulatoryTempFinal101("MEDIUM_REGULATORY_COMPLIANCE"),
	      regulatoryTempFinal101("HIGH_REGULATORY_COMPLIANCE"),
	      regulatoryTempFinal101("VERY_HIGH_REGULATORY_COMPLIANCE"),
        regulatoryTempFinal101("LOAD_SERVING_CAPABILITY_SCORE"),
        regulatoryTempFinal101("REGULATORY_COMPLIANCE_SCORE"),
	      col("current_date").alias("EXECUTION_DATE"))
  
      val regulatoryTempFinal103 = regulatoryTempFinal102.na.fill(1, Array("LOAD_SERVING_CAPABILITY_SCORE", "REGULATORY_COMPLIANCE_SCORE"))
      loadTable(List("loadServingRegulatoryComplianceL1","dimCharacteristicsL1"), "factLoadServingRegulatoryComplianceL2" )
   
    }
  }

  val loadFactCustomerCountL2: Transformation = {
      case dimCustomerCountL1 :: dimCharacteristicsL1 :: Nil => {
        val customerTempFinal100=dimCustomerCountL1.select(
          col("EQUIP"),
          col("SUBSTATION"),
          col("ASSET_DX"),
          col("PRIMARY_VOLTAGE"),
          col("SECONDARY_VOLTAGE"),
          col("SUM_OF_CUSTOMERS"),
          col("file_year"),
          col("file_month"))

        val customerTempFinal101 = customerTempFinal100
          .withColumn("CUSTOMER_IMPACT", when(col("SUM_OF_CUSTOMERS") === 0, 1).when(col("SUM_OF_CUSTOMERS") <= 499, 2)
          .when(col("SUM_OF_CUSTOMERS") <= 5000, 3).when(col("SUM_OF_CUSTOMERS") > 5000, 4))
    
        val customerTempFinal102 = customerTempFinal101
          .join(dimCharacteristicsL1, Seq("EQUIP"), "right")
          .select(
          dimCharacteristicsL1("EQUIP"),
          customerTempFinal101("SUBSTATION"),
          customerTempFinal101("ASSET_DX"),
          customerTempFinal101("PRIMARY_VOLTAGE"),
          customerTempFinal101("SECONDARY_VOLTAGE"),
          customerTempFinal101("SUM_OF_CUSTOMERS"),
          customerTempFinal101("CUSTOMER_IMPACT"),
          col("current_date").alias("EXECUTION_DATE"))
      }

  }

  val loadFactQualityOfSupplyL3: Transformation = {
       case factCharacteristicsL2 :: factCustomerCountL2 :: factLoadServingRegulatoryComplianceL2 :: factQualityOfSupplyL3 :: Nil => {
       val qualSuppSapTempFinal=factCharacteristicsL2.select(
         col("EQUIP"),
         col("SERIAL_NUM"),
         col("SYSTEM_RESTORATION"),
         col("REPUTATION"),
         col("FILE_YEAR AS FILE_YEAR_CHARACTERISTICS"),
         col("FILE_MONTH AS FILE_MONTH_CHARACTERISTICS"))
     
   
   val qualSuppCustTempFinal=factCustomerCountL2.select(
      col("EQUIP"),
      col("CUSTOMER_IMPACT"),
      col("FILE_YEAR AS FILE_YEAR_CUSTOMER"),
      col("FILE_MONTH AS FILE_MONTH_CUSTOMER")
    )

   val qualSuppRegulTempFinal=factLoadServingRegulatoryComplianceL2.select(
    col("EQUIP"),
    col("LOAD_SERVING_CAPABILITY_SCORE"),
    col("REGULATORY_COMPLIANCE_SCORE"),
    col("FILE_YEAR AS FILE_YEAR_LOAD_SERVING_REGULATORY"),
    col("FILE_MONTH AS FILE_MONTH_LOAD_SERVING_REGULATORY")
   )

    val qualitySupp = qualSuppSapTempFinal.join(qualSuppCustTempFinal, Seq("EQUIP"), "left")
                                          .join(qualSuppRegulTempFinal, Seq("EQUIP"), "left")
                                          .withColumn("QUALITY_OF_SUPPLY_SCORE", greatest("CUSTOMER_IMPACT", "LOAD_SERVING_CAPABILITY_SCORE",
                                                      "SYSTEM_RESTORATION", "REGULATORY_COMPLIANCE_SCORE", "REPUTATION")
                                                      )
    val qualitySupp101= qualitySupp.select(
      col("EQUIP"),
      col("SERIAL_NUM"),
      col("SYSTEM_RESTORATION"),
      col("REPUTATION"),
      col("CUSTOMER_IMPACT"),
      col("LOAD_SERVING_CAPABILITY_SCORE"),
      col("REGULATORY_COMPLIANCE_SCORE"),
      col("QUALITY_OF_SUPPLY_SCORE"),
      col("FILE_YEAR_CHARACTERISTICS"),
      col("FILE_MONTH_CHARACTERISTICS"),
      col("FILE_YEAR_CUSTOMER"),
      col("FILE_MONTH_CUSTOMER"),
      col("FILE_YEAR_LOAD_SERVING_REGULATORY"),
      col("FILE_MONTH_LOAD_SERVING_REGULATORY"))

    }
  }                                    

  val loadFactCriticalityScoreL4: Transformation = {
    case factCharacteristicsL2:: factQualityOfSupplyL3 :: factCriticalityScoreL4: Nil => {
      val criticalitySapTempFinal=factCharacteristicsL2.select(
        col("EQUIP"),
        col("SAFETY"),
        col("ENVIRONMENTAL"),
        col("FINANCIAL"),
        col("EXECUTION_DATE").alias("EXECUTION_DATE_SAP_CHARACTERISTICS")

      )

      val criticalityQualityTempFinal=factQualityOfSupplyL3.select(
        col("EQUIP"),
        col("QUALITY_OF_SUPPLY_SCORE"),
        col("EXECUTION_DATE").alias("EXECUTION_DATE_QUALITY_OF_SUPPLY")
      )

      val critScore = criticalitySapTempFinal.join(criticalityQualityTempFinal, Seq("EQUIP"), "left")
                          .withColumn("CRITICALITY_SCORE", when($"SAFETY".isNotNull, $"SAFETY").otherwise(0) * 0.25
                          + when($"ENVIRONMENTAL".isNotNull, $"ENVIRONMENTAL").otherwise(0) * 0.15
                          + when($"QUALITY_OF_SUPPLY_SCORE".isNotNull, $"QUALITY_OF_SUPPLY_SCORE").otherwise(0) * 0.3 + $"FINANCIAL" * 0.3)

      val critScore2 = critScore.withColumn("CRITICALITY_SCORE", round(col("CRITICALITY_SCORE"), 1))

      val critScore3 = critScore2.select(
        col("EQUIP"),
        col("SAFETY"),
        col("ENVIRONMENTAL"),
        col("QUALITY_OF_SUPPLY_SCORE"),
        col("FINANCIAL"),
        round(col("CRITICALITY_SCORE"),2).alias("CRITICALITY_SCORE"),
        col("EXECUTION_DATE_QUALITY_OF_SUPPLY"),
        col("EXECUTION_DATE_SAP_CHARACTERISTICS")       
      )

    }
  }

 