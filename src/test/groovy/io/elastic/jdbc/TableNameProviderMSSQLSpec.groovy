package io.elastic.jdbc

import javax.json.Json
import javax.json.JsonObject
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class TableNameProviderMSSQLSpec extends Specification {

    @Shared
    def connectionString = ""
    @Shared
    def user = ""
    @Shared
    def password = ""
    @Shared
    def databaseName = ""

    @Shared
    Connection connection

    @Shared
    JsonObject config

    def setupSpec() {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(connectionString, user, password)
        config = Json.createObjectBuilder().build()
        config = Json.createObjectBuilder().add("user", user)
                                         .add("password", password)
                                         .add("dbEngine", "mssql")
                                         .add("host", "")
                                         .add("databaseName", databaseName).build()

        connection.createStatement().execute(" DROP TABLE IF EXISTS users");
        connection.createStatement().execute(" DROP TABLE IF EXISTS products");
        connection.createStatement().execute(" DROP TABLE IF EXISTS orders");
    }

    def cleanupSpec() {
        connection.createStatement().execute(" DROP TABLE IF EXISTS users");
        connection.createStatement().execute(" DROP TABLE IF EXISTS products");
        connection.createStatement().execute(" DROP TABLE IF EXISTS orders");

        connection.close();
    }

    def "create tables, successful"() {

        JsonObjectBuilder config = Json.createObjectBuilder()
        config.add("user", user)
              .add("password", password)
              .add("dbEngine", "mssql")
              .add("host", "")
              .add("port", "")
              .add("databaseName", databaseName)
              .add("tableName", "stars")

        String sql1 = "CREATE TABLE users (id int, name varchar(255) NOT NULL, radius int, destination int)"
        String sql2 = "CREATE TABLE products (id int, name varchar(255) NOT NULL, radius int, destination int)"
        String sql3 = "CREATE TABLE orders (id int, name varchar(255) NOT NULL, radius int, destination int)"

        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);


        TableNameProvider provider = new TableNameProvider();

        when:
        JsonObject model = provider.getSelectModel(config.build());

        then:
        print model
        model.toString().contains('"dbo.orders":"dbo.orders","dbo.products":"dbo.products","dbo.users":"dbo.users"')
    }
}
