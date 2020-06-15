package com.aadhaarcard
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class AadhaarCard(aadhaarGenerated: Int, age:Int, date:String, district:String, emailCount:Int,
    gender:String, mobileCount:Int, pincode:Option[Long], privateAgency:String, registrar:String,
    rejected:Int, state:String, subDistrict:String)

object AadhaarCardAnalysis {
def main(args:Array[String])
  {
  val spark=SparkSession
  .builder()
  .master("local")
  .getOrCreate()
  
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  val aadhaarSchema=StructType(Array(StructField("date",StringType,true), StructField("registrar",StringType,true), StructField("privateAgency",StringType,true), 
                    StructField("state",StringType,true), StructField("district",StringType,true), StructField("subDistrict",StringType,true), 
                    StructField("pincode",LongType,true), StructField("gender",StringType,true), StructField("age",IntegerType,true), 
                    StructField("aadhaarGenerated",IntegerType,true), StructField("rejected",IntegerType,true), StructField("mobileCount",IntegerType,true), 
                    StructField("emailCount",IntegerType,true)))

aadhaarSchema.printTreeString
val path="C:/Users/Charan/Desktop/AadhaarCardAnalysis_SparkSQL_DF/aadhaarCard_data.csv"
val aadhaarDataDS=spark.read.schema(aadhaarSchema).csv(path).as[AadhaarCard]
//aadhaarDataDS.show()

  //KPI-1 
  //1. View/result of the top 25 rows from each individual store
  
aadhaarDataDS.groupByKey(data=>data.privateAgency).count().show(25)

//KPI-2
// 1. Find the count and names of registrars in the table.

aadhaarDataDS.groupByKey(row=>row.registrar).count().show()

//2. Find the number of states, districts in each state and sub-districts in each district. 

aadhaarDataDS.groupBy($"state").agg(countDistinct("district"),countDistinct("subDistrict")).show()

//3. Find the number of males and females in each state from the table

//One way
 aadhaarDataDS.filter(col("gender")==="M").groupBy($"state").count().show()
 aadhaarDataDS.filter(col("gender")==="F").groupBy($"state").count().show()
 //second way
 aadhaarDataDS.filter(data=>data.gender=="F").groupByKey(data=>data.state).count().show()
 aadhaarDataDS.filter(data=>data.gender=="M").groupByKey(data=>data.state).count().show()
 
 //4. Find out the names of private agencies for each state
//one way
aadhaarDataDS.groupByKey(l=>l.state).agg(count($"privateAgency").as[String]).show()
//second way
aadhaarDataDS.select($"privateAgency").distinct().show()
 
//KPI-3
// 1. Find top 3 states generating most number of Aadhaar cards?
aadhaarDataDS.groupBy($"state").agg(sum($"aadhaarGenerated").as[Long].as("TotalaadhaarGenerated")).orderBy(desc("TotalaadhaarGenerated")).show(3)

// 2. Find top 3 private agencies generating the most number of Aadhar cards?
aadhaarDataDS.groupBy($"privateAgency").agg(sum($"aadhaarGenerated").as[Long].as("TotalaadhaarGenerated")).orderBy(desc("TotalaadhaarGenerated")).show(3)

//3. Find the number of residents providing email, mobile number? (Hint: consider non-zero values.) 
//Mobile Count 
aadhaarDataDS.groupBy($"state").agg(sum($"mobileCount").as[Long].as("TotalMobileCount")).orderBy(desc("TotalMobileCount")).show()
 //Email Count
 aadhaarDataDS.groupBy($"state").agg(sum($"emailCount").as[Long].as("TotalEmailCount")).orderBy(desc("TotalEmailCount")).show()
  
 // 4. Find top 3 districts where enrolment numbers are maximum?
 aadhaarDataDS.groupBy($"district").agg((sum($"aadhaarGenerated").as[Long]+sum($"rejected").as[Long]).as("TotalEnrollement")).orderBy(desc("TotalEnrollement")).show(3)
  
  //5. Find the no. of Aadhaar cards generated in each state?
  aadhaarDataDS.groupBy($"state").agg(sum($"aadhaarGenerated").as[Long].as("TotalaadhaarGenerated")).orderBy(desc("TotalaadhaarGenerated")).show()
  
  //KPI-4 
  // 2. Find the number of unique pincodes in the data?
 aadhaarDataDS.agg(countDistinct($"pincode")).show()
 
 //3. Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra? 
   aadhaarDataDS.filter(data=>data.state=="Uttar Pradesh").agg(sum($"rejected").as[Long]).show()
   aadhaarDataDS.filter(data=>data.state=="Maharashtra").agg(sum($"rejected").as[Long]).show()

  aadhaarDataDS.createOrReplaceTempView("aadhaar")
  
  //1. Write a command to see the correlation between “age” and “mobile_number”? (Hint: Consider the percentage of people who have provided the mobile number out of the total applicants) 
   spark.sqlContext.sql("select age1.age, (mobilecount*100/TotalApplicants) as MobilePercentage from (select age, sum(mobileCount) as mobilecount from aadhaar group by age) as age1 join (select age, (sum(aadhaarGenerated)+sum(rejected)) as TotalApplicants from aadhaar group by age ) as age2 on age1.age==age2.age order by MobilePercentage DESC ").show();
  
  //KPI-5
 // 1. The top 3 states where the percentage of Aadhaar cards being generated for males is the highest.  
  spark.sqlContext.sql("select per1.state,(male_per*100/total_per) as percentage from (select state, sum(aadhaarGenerated) as male_per from aadhaar where gender='M' group by state) as per1 join (select state,sum(aadhaarGenerated) total_per from aadhaar group by state) as per2 on per1.state=per2.state order by percentage DESC LIMIT 3").show()
  
  // 2. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
  spark.sqlContext.sql("select per1.district,(female_per*100/total_per) as percentage from (select district, sum(rejected) as female_per from aadhaar where gender='F' AND state='Manipur' group by district) as per1 join (select district,sum(rejected) total_per from aadhaar where state='Manipur' group by district ) as per2 on per1.district=per2.district order by percentage DESC LIMIT 3").show()
  
  //3. The top 3 states where the percentage of Aadhaar cards being generated for females is the highest. 
  spark.sqlContext.sql("select per1.state,(female_per*100/total_per) as percentage from (select state, sum(aadhaarGenerated) as female_per from aadhaar where gender='F' group by state) as per1 join (select state,sum(aadhaarGenerated) total_per from aadhaar group by state) as per2 on per1.state=per2.state order by percentage DESC LIMIT 3").show()
  
  //4. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for males is the highest
  spark.sqlContext.sql("select per1.district,(male_per*100/total_per) as percentage from (select district, sum(rejected) as male_per from aadhaar where gender='M' AND state='Andaman and Nicobar Islands' group by district) as per1 join (select district,sum(rejected) total_per from aadhaar where state='Andaman and Nicobar Islands' group by district ) as per2 on per1.district=per2.district order by percentage DESC LIMIT 3").show()
  
  spark.stop()

  }
  
  
  
  
  
  
  
  


}