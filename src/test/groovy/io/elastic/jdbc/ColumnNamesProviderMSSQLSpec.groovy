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
class ColumnNamesProviderMSSQLSpec extends Specification {
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

    def setup() {
        connection = DriverManager.getConnection(connectionString, user, password);
        String sql = " DROP TABLE IF EXISTS stars;"
        connection.createStatement().execute(sql)
        sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME)"
        connection.createStatement().execute(sql);
    }

    def cleanupSpec() {
        String sql = " DROP TABLE IF EXISTS stars;"
        connection.createStatement().execute(sql)
        connection.close()
    }

    def "get metadata model, given table name"() {

        JsonObjectBuilder config = Json.createObjectBuilder()
        config.add("user", user)
              .add("password", password)
              .add("dbEngine", "mssql")
              .add("host", "")
              .add("port", "")
              .add("databaseName", databaseName)
              .add("tableName", "stars")
        ColumnNamesProvider provider = new ColumnNamesProvider()
        JsonObject meta = provider.getMetaModel(config.build())
        print meta
        expect:
        meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":false,\"title\":\"ID\",\"type\":\"number\"},\"name\":{\"required\":true,\"title\":\"name\",\"type\":\"string\"},\"radius\":{\"required\":false,\"title\":\"radius\",\"type\":\"number\"},\"destination\":{\"required\":false,\"title\":\"destination\",\"type\":\"number\"},\"createdat\":{\"required\":false,\"title\":\"createdat\",\"type\":\"string\"}}}}"
    }
}
