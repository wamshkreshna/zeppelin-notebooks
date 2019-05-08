import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel._; 
import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object businessRuleExclusion {

def main(args: Array[String]) {
val sparkConf = new SparkConf().setAppName("BusinessRuleExclusion")
val sc = new SparkContext(sparkConf)
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
import sqlContext.implicits._

val df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(args(0)) ///prod/hyd/intake/rp2/ingestion/config/BUSINESSRULE_EXCLUSION.csv 

df.createOrReplaceTempView("businessrule_exclusion")
df.show
//MY SQL
val mysqlDriver="com.mysql.jdbc.Driver"
val mysqlUrl="jdbc:mysql://10.131.40.94:3306"
val mysqlDatabase="audit_hcdlprd"
val mysqlUsername="cringadmin"
val key = "This is my key! Don't steal"
val mysqlPassword=Encryption.decrypt(key, "0sqasQBspFZJR4PeyY58fg==")


val config = new BoneCPConfig()
config.setJdbcUrl(mysqlUrl)
config.setUsername(mysqlUsername)
config.setPassword(mysqlPassword)
config.setMinConnectionsPerPartition(2)
config.setMaxConnectionsPerPartition(5)
config.setPartitionCount(3)
val connectionPool = new BoneCP(config); // setup the connection pool
val connection = connectionPool.getConnection();


val schemaflg = sqlContext.sql("select src_schema from businessrule_exclusion where domain_rule='SCHEMA'")
schemaflg.show
if(schemaflg.count >0){
      schemaflg.collect.foreach{x=>
      println("Data present=>"+x.getString(0))  
      val schema_name= x.getString(0)     
      val updateAllps = connection.prepareStatement(
      "UPDATE audit_hcdlprd.obj_metadata  set active_flg='N' where  schema_name=?");
      updateAllps.setString(1,schema_name);
      updateAllps.execute()

    }
}
val inActiveTables = sqlContext.sql("select src_schema, table_rule  from businessrule_exclusion where rule_type='E' and domain_rule is null")
val ps = connection.prepareStatement(
             "UPDATE audit_hcdlprd.obj_metadata  set active_flg='N' where obj_name=? and schema_name=?");
inActiveTables.show

             inActiveTables.collect.foreach{row=>    
             if(connection!= null){
             println("suceess")
            
              ps.setString(1,row.getString(1));
              ps.setString(2,row.getString(0));
              ps.execute()
             
               
             }
            }

connection.close()
connectionPool.shutdown
}
}


