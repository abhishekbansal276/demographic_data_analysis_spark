import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

case class Person(aadhaar_id: String, person_name: Name, gender: String, age: Double, dob: String, occupation: Option[String], monthly_income: Option[Double], phone: String)
case class Name(first_name: String, second_name: String)
case class Address(street: String, city: String, state: String, postalCode: String)
case class Household(aadhaar_id: String, father_name: String, address: Address, monthly_income: Option[Double], category: Option[String], family_members: Array[String])

object BroadcastExample extends App {
  val spark = SparkSession.builder()
    .appName("BroadcastExample")
    .master("local[*]")
    .getOrCreate()

  private val sc = spark.sparkContext

  val personsJsonFilePath = "D:\\scala_play_projects\\untitled2\\persons.json"
  val householdsJsonFilePath = "D:\\scala_play_projects\\untitled2\\households.json"

  val personsRdd: RDD[Person] = spark.read.json(personsJsonFilePath).rdd.map { row =>
    val aadhaar_id = row.getAs[String]("aadhaar_id")
    val person_name = row.getAs[org.apache.spark.sql.Row]("person_name")
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

  val householdsRdd: RDD[Household] = spark.read.json(householdsJsonFilePath).rdd.map { row =>
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

  // Enrich the household data with information about the persons living in each household

  // Broadcast both RDDs
  private val broadcastPersons: Broadcast[Array[Person]] = sc.broadcast(personsRdd.collect())
  private val broadcastHouseholds: Broadcast[Array[Household]] = sc.broadcast(householdsRdd.collect())

  // Households where the head of the household is an engineer
  private val householdsWithEngineerHead: RDD[Household] = sc.parallelize(broadcastHouseholds.value).filter { household =>
    val head = household.family_members.head
    broadcastPersons.value.exists(_.aadhaar_id == head) && broadcastPersons.value.find(_.aadhaar_id == head).get.occupation.contains("Engineer")
  }

  // Households where there are at least two persons above the age of 30
  private val householdsWithTwoPersonsAbove30: RDD[Household] = sc.parallelize(broadcastHouseholds.value).filter { household =>
    val personsAbove30 = household.family_members.count { person =>
      val age = broadcastPersons.value.find(_.aadhaar_id == person).map(_.age)
      age.exists(_ > 30)
    }
    personsAbove30 >= 2
  }

  // Households where the head of the household is a male and is a teacher
  private val householdsWithMaleTeacherHead: RDD[Household] = sc.parallelize(broadcastHouseholds.value).filter { household =>
    val head = household.family_members.headOption
    val headPerson = head.flatMap(id => broadcastPersons.value.find(_.aadhaar_id == id))
    headPerson.exists(person => person.gender == "Male" && person.occupation.contains("Teacher"))
  }

  println("Households with engineer head:")
  householdsWithEngineerHead.collect().foreach(println)

  println("Households with at least two persons above the age of 30:")
  householdsWithTwoPersonsAbove30.collect().foreach(println)

  println("Households with male teacher head:")
  householdsWithMaleTeacherHead.collect().foreach(println)

  Thread.sleep(60000)

  spark.stop()
}