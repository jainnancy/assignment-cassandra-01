package edu.knoldus

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.ConfigFactory

trait CassandraProvider {

  val config = ConfigFactory.load()
  val cassandraKeyspace = config.getString("cassandra.keyspace")
  val cassandraHostname = config.getString("cassandra.contact.points")

  val cassandraSession: Session = {
    val cluster = new Cluster.Builder().withClusterName("Assignment cluster")
      .addContactPoint(cassandraHostname).build()
    val session = cluster.connect()
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspace} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    println(s"Keyspace ${cassandraKeyspace} created...")
    session.execute(s"USE ${cassandraKeyspace}")
    println(s"Using keyspace ${cassandraKeyspace}...")
    session
  }
}
