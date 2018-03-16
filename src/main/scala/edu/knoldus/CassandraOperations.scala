package edu.knoldus

import scala.collection.JavaConverters._

object CassandraOperations extends App with CassandraProvider {

  dropTable();
  createTable();
  insertEmployee();
  getEmployeeDetailsById(1) // 1.
  updateDetailsByIdNSalary(1,50000) //2.
  getDetailsByIdNSalary() //3.
  //getEmployeeDetailsByCity("Chandigarh") //4.

  def dropTable() = {
    cassandraSession.execute(s"Drop table IF EXISTS employee")
  }

  def createTable() = {
    cassandraSession.execute(s"create TABLE IF NOT EXISTS employee(emp_id int, emp_name text, emp_city text, emp_salary varint, emp_phone varint, PRIMARY KEY(emp_id,emp_salary))")
    println("\nEmployee table created...")
    }

  def insertEmployee() = {
    cassandraSession.execute(s"INSERT INTO employee(emp_id, emp_city, emp_name, emp_salary, emp_phone) VALUES(1,'Name1','Delhi',50000,9876543210)")
    cassandraSession.execute(s"INSERT INTO employee(emp_id, emp_city, emp_name, emp_salary, emp_phone) VALUES ( 4,'Delhi','Nancy',30000,9876547890)")
    cassandraSession.execute(s"INSERT INTO employee(emp_id, emp_city, emp_name, emp_salary, emp_phone) VALUES ( 2,'Vasundhra','Shivangi',34000,9276556490)")
    cassandraSession.execute(s"INSERT INTO employee(emp_id, emp_city, emp_name, emp_salary, emp_phone) VALUES ( 3,'Faridabad','Anmol',40000,9271236490)")
    cassandraSession.execute(s"INSERT INTO employee(emp_id, emp_city, emp_name, emp_salary, emp_phone) VALUES ( 5,'Chandigarh','Neha',32000,9214336486)")
    println("\nEmployee details inserted...")
  }

  def getEmployeeDetailsById(id: Int) {
    cassandraSession.execute(s"create TABLE IF NOT EXISTS employees(emp_id int, emp_name text, emp_city text, emp_salary varint, emp_phone varint,PRIMARY KEY(emp_id,emp_salary))")
    val result = cassandraSession.execute(s"SELECT * FROM employee WHERE emp_id = $id").asScala.toList
    println(s"\n1. Employee details with emp_id = ${id}:")
    for (data <- result) {
      println(data);
    }
  }

  def getEmployeeDetailsByCity(city: String) = {
    cassandraSession.execute(s"CREATE INDEX  on employee(emp_city)")
    //Thread.sleep(1000)
    val result = cassandraSession.execute(s"SELECT * FROM employee WHERE emp_city = '$city'").asScala.toList
    println(s"\n4. Employee details with city = ${city}:")
    for (data <- result) {
      println(data)
    }
  }

  def updateDetailsByIdNSalary(id: Int, salary: Int) = {

    cassandraSession.execute(s"UPDATE employee SET emp_city = 'chandigarh' where emp_id = $id and emp_salary = $salary")
    println(s"\n2. Updated city by id and salary")
  }

  def getDetailsByIdNSalary() = {
    val result = cassandraSession.execute(s"SELECT * FROM employee where emp_id = 1 and emp_salary > 30000").asScala.toList
    println(s"\n3. Employee details by id = 1 and salary > 30000:")
    for (data <- result) {
      println(data)
    }
  }
}
