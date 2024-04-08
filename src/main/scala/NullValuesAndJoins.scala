import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object NullValuesAndJoins extends App {
  val spark = SparkSession.builder()
    .appName("NullValuesAndJoinsRDD")
    .master("local[*]")
    .getOrCreate()

  val personsJsonFilePath = "D:\\scala_play_projects\\untitled2\\persons.json"
  val householdsJsonFilePath = "D:\\scala_play_projects\\untitled2\\households.json"

  val sc = spark.sparkContext

  private val personsRdd: RDD[Person] = spark.read.json(personsJsonFilePath)
    .rdd
    .map { row =>
      val aadhaar_id = row.getAs[String]("aadhaar_id")
      val person_name = row.getAs[Row]("person_name")
      val name = Name(
        first_name = person_name.getAs[String]("first_name"),
        second_name = person_name.getAs[String]("second_name")
      )
      val gender = row.getAs[String]("gender")
      val age = row.getAs[Double]("age")
      val dob = row.getAs[String]("dob")
      val occupation = Option(row.getAs[String]("occupation")).getOrElse("N/A")
      val monthly_income = Option(row.getAs[Double]("monthly_income")).getOrElse(0.0)
      val phone = row.getAs[String]("phone")

      Person(aadhaar_id, name, gender, age, dob, Some(occupation), Some(monthly_income), phone)
    }

  private val householdsRdd: RDD[Household] = spark.read.json(householdsJsonFilePath)
    .rdd
    .map { row =>
      val aadhaar_id = row.getAs[String]("aadhaar_id")
      val father_name = row.getAs[String]("father_name")
      val address = row.getAs[Row]("address")
      val street = address.getAs[String]("street")
      val city = address.getAs[String]("city")
      val state = address.getAs[String]("state")
      val postalCode = address.getAs[String]("postalCode")
      val monthly_income = Option(row.getAs[Double]("monthly_income")).getOrElse(0.0)
      val category = Option(row.getAs[String]("category")).getOrElse("N/A")
      val family_members = row.getAs[Seq[String]]("family_members").toArray

      val addressObj = Address(street, city, state, postalCode)
      Household(aadhaar_id, father_name, addressObj, Some(monthly_income), Some(category), family_members)
    }

  println("Persons RDD:")
  personsRdd.take(10).foreach(println)

  println("\nHouseholds RDD:")
  householdsRdd.take(10).foreach(println)

  private val innerJoinRdd: RDD[(String, (Person, Household))] = personsRdd.map(person => (person.aadhaar_id, person))
    .join(householdsRdd.map(household => (household.aadhaar_id, household)))

  private val leftOuterJoinRdd: RDD[(String, (Person, Option[Household]))] = personsRdd.map(person => (person.aadhaar_id, person))
    .leftOuterJoin(householdsRdd.map(household => (household.aadhaar_id, household)))

  private val rightOuterJoinRdd: RDD[(String, (Option[Person], Household))] = personsRdd.map(person => (person.aadhaar_id, person))
    .rightOuterJoin(householdsRdd.map(household => (household.aadhaar_id, household)))

  private val personsAbove30Rdd: RDD[Person] = personsRdd.filter(_.age > 30)

  private val addressesInCityRdd: RDD[Address] = householdsRdd.map(_.address).filter(_.city == "YourCity")

  private val fullNamesRdd: RDD[String] = personsRdd.map(person => s"${person.person_name.first_name} ${person.person_name.second_name}")

  private val countByAgeRdd: RDD[(Double, Int)] = personsRdd.map(person => (person.age, 1))
    .reduceByKey(_ + _)

  println("\nInner Join Result:")
  innerJoinRdd.take(10).foreach(println)

  println("\nLeft Outer Join Result:")
  leftOuterJoinRdd.take(10).foreach(println)

  println("\nRight Outer Join Result:")
  rightOuterJoinRdd.take(10).foreach(println)

  println("\nPersons aged above 30:")
  personsAbove30Rdd.take(10).foreach(println)

  println("\nAddresses in YourCity:")
  addressesInCityRdd.take(10).foreach(println)

  println("\nFull Names of Persons:")
  fullNamesRdd.take(10).foreach(println)

  println("\nCount of Persons per Age:")
  countByAgeRdd.collect().foreach(println)

  Thread.sleep(60000)

  spark.stop()
}