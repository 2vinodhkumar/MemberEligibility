package com.members

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, concat_ws}

object MembersEligibility {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Eligibility").getOrCreate()
    val members = spark.read.option("header","true").csv("/Users/vinodhkumark/SIEMData/citiustech/member_months.csv")
    val eligibility = spark.read.option("header", "true").csv("/Users/vinodhkumark/SIEMData/citiustech/member_eligibility.csv")

    /**
     * calculate the total number of members months.
     * The resulting set should contain the member's ID, full name, along with the number of member months. Output the report in json, partitioned by the membersID.
     */
    val eligibility_with_fn = eligibility.
                              withColumn("fullname",concat_ws(" ",
                                                          col("first_name"),
                                col("middle_name"),col("last_name"))).
                              drop("first_name","middle_name","last_name")

    val num_member_months = members.groupBy("member_id").count()

    /**
     * Adding broadcas hint on join for performance improvement
     */
    val member_months = eligibility_with_fn.join(broadcast(num_member_months), num_member_months(
    "member_id")===eligibility_with_fn("member_id"),"inner").drop(num_member_months("member_id")).withColumnRenamed("count","member_months")

    member_months.show()
    member_months.write.partitionBy("member_id").json("/Users/vinodhkumark/SIEMData/citiustech/member_months")

    /**
     * calculate the total number of member months per member per year.
     * The resulting set should contain the member's ID, the month, and total number of member months. Output the result set in json
     *
     * The window here is on year and member id, selected attributes are month ,member and total number of member months.
     * If we have to calculate the total number of member months per month per member then on window spec pass mont instead of year
     */

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions._

    val members_with_year_month = members.withColumn(
    "eligibility_effective_year",year(members("eligiblity_effective_date"))).withColumn("eligibility_effective_month",month(members("eligiblity_effective_date")))


    val ws = Window.partitionBy("eligibility_effective_year","member_id")

    val total_member_months_per_year_per_memberr = members_with_year_month.withColumn("total_member_months_per_year",count("eligbility_number").over(ws))

    val member_months_per_year = total_member_months_per_year_per_memberr.select("member_id", "eligibility_effective_month", "total_member_months_per_year").withColumnRenamed("eligibility_effective_month", "month").withColumnRenamed("total_member_months_per_year", "member_months_per_year")

    member_months_per_year.show()
    member_months_per_year.write.json("/Users/vinodhkumark/SIEMData/citiustech/member_months_per_year")
  }
}
