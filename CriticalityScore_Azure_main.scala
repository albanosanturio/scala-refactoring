%run AsstMgmt/CriticalityScoreTT
%run AsstMgmt/Utils


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession


object CriticalityScore {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString(
      mssparkutils.fs.head(args(0))).getConfig("general")
    val dataStoreName = config.getConfig("dbs")
    val dbName = dbs.getString("am_db")

    val loadFromDatastore = loadTable(dataStoreName, dbName)
    val loadFromDB = loadTable(None, dbName)
    
    val inputLoadDimCustomerCountL1 = List(
      Table("customer_count", YearMonth()))
    
    val outputLoadDimCustomerCountL1 = Table("dim_customer_count_l1", YearMonth())
    loadFromDataStore(inputLoadDimCustomerCountL1, outputLoadDimCustomerCountL1, dimCustomerCountL1)
    
	  val inputLoadServingRegulatoryComplianceL1 = List(
      Table("load_serving_regulatory_compliance", YearMonth())
    )
    
	  val outputLoadServingRegulatoryComplianceL1 = Table("dim_load_serving_regulatory_compliance_l1", YearMonth())
    loadFromDataStore(inputLoadServingRegulatoryComplianceL1, outputLoadServingRegulatoryComplianceL1, dimLoadServingRegulatoryComplianceL1)

    val inputFactLoadServingRegulatoryComplianceL2 = List(
      Table("dim_load_serving_regulatory_compliance_l1", YearMonth()),
			Table("dim_characteristics_l1", YearMonth())
	  )
	
	  val outputFactLoadServingRegulatoryComplianceL2 = Table("fact_load_serving_regulatory_compliance_l2", YearMonth())
    loadFromDB(inputFactLoadServingRegulatoryComplianceL2, outputFactLoadServingRegulatoryComplianceL2, factLoadServingRegulatoryComplianceL2)

	  val inputFactCustomerCountL2 = List(
      Table("dim_customer_count_l1", YearMonth()),
			Table("dim_characteristics_l1", YearMonth())
	  )
	
	val outputFactCustomerCountL2 = Table("fact_customer_count_l2", YearMonth())
    loadFromDB(inputFactCustomerCountL2, outputFactCustomerCountL2, loadFactCustomerCountL2)
	 	
  val inputFactQualityOfSupplyL3 = List(
      Table("fact_characteristics_l2", YearMonth()),
			Table("fact_customer_count_l2", YearMonth()),
			Table("fact_load_serving_regulatory_compliance_l2", YearMonth()),
	  )
	
	val outputFactQualityOfSupplyL3 = Table("fact_quality_of_supply_l3", ExecutionDate())
    loadFromDB(inputFactCustomerCountL2, outputFactCustomerCountL2, loadFactQualityOfSupplyL3)

	val inputFactCriticalityscoreL4 = List(
      Table("fact_characteristics_l2", ExecutionDate()),
			Table("fact_quality_of_supply_l3", ExecutionDate())
	  )
	
	val outputFactQualityOfSupplyL3 = Table("fact_criticalityscore_l4", ExecutionDate())
    loadFromDB(inputFactCriticalityscoreL4, outputFactQualityOfSupplyL3, loadFactCriticalityscoreL4) 
  
    spark.stop()
  }
}
