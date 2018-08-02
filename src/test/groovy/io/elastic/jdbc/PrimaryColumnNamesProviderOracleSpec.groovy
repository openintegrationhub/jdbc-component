package io.elastic.jdbc

import spock.lang.*

import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class PrimaryColumnNamesProviderOracleSpec extends Specification {
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
        String sql = "BEGIN\n" +
                     "   EXECUTE IMMEDIATE 'DROP TABLE stars';\n" +
                     "EXCEPTION\n" +
                     "   WHEN OTHERS THEN\n" +
                     "      IF SQLCODE != -942 THEN\n" +
                     "         RAISE;\n" +
                     "      END IF;\n" +
                     "END;"
        connection.createStatement().execute(sql)
        sql = "CREATE TABLE\n" +
                "    ELASTICIO.stars\n" +
                "    (\n" +
                "        ID INTEGER,\n" +
                "        name VARCHAR2(255) NOT NULL,\n" +
                "        raduis INTEGER,\n" +
                "        destination FLOAT,\n" +
                "        createdat DATE,\n" +
                "        PRIMARY KEY (ID)\n" +
                "    )"
        connection.createStatement().execute(sql);
    }

    def cleanupSpec() {
        String sql = "BEGIN\n" +
                     "   EXECUTE IMMEDIATE 'DROP TABLE stars';\n" +
                     "EXCEPTION\n" +
                     "   WHEN OTHERS THEN\n" +
                     "      IF SQLCODE != -942 THEN\n" +
                     "         RAISE;\n" +
                     "      END IF;\n" +
                     "END;"
        connection.createStatement().execute(sql)
        connection.close()
    }

    def "get metadata model, given table name"() {

        JsonObjectBuilder config = Json.createObjectBuilder()
        config.add("user", user)
        .add("password", password)
        .add("dbEngine", "oracle")
        .add("host", "")
        .add("port", "")
        .add("databaseName", databaseName)
        .add("tableName", "stars")
        PrimaryColumnNamesProvider provider = new PrimaryColumnNamesProvider()
        JsonObject meta = provider.getMetaModel(config.build());
        print meta
        expect:
        meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"ID\":{\"required\":true,\"title\":\"ID\",\"type\":\"number\"}}}}"
    }
}
