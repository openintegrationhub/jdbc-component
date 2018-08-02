package io.elastic.jdbc.triggers

import javax.json.JsonObject
import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import spock.lang.*

import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Matcher

@Ignore
class SelectTriggerMSSQLSpec extends Specification {

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

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    LocalDateTime now = LocalDateTime.now()
    String today = dtf.format(now)

    def setup() {
        connection = DriverManager.getConnection(connectionString, user, password)

        String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
                "  DROP TABLE stars;"
        connection.createStatement().execute(sql)

        sql = "CREATE TABLE stars (ID int, name varchar(255) NOT NULL, radius int, destination float, createdat DATETIME)"
        connection.createStatement().execute(sql)

        sql = "INSERT INTO stars (ID, name, radius, destination, createdat) VALUES (1, 'Sun', 50, 170, '" + today + " 10:00:00')"
        connection.createStatement().execute(sql)
    }

    def cleanupSpec() {
        String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
                "  DROP TABLE stars;"
        connection.createStatement().execute(sql)
        connection.close()
    }

    def "make a SELECT request"() {

        Callback errorCallback = Mock(Callback)
        Callback snapshotCallback = Mock(Callback)
        Callback dataCallback = Mock(Callback)
        Callback onreboundCallback = Mock(Callback)

        EventEmitter emitter = new EventEmitter.Builder()
                .onData(dataCallback)
                .onSnapshot(snapshotCallback)
                .onError(errorCallback)
                .onRebound(onreboundCallback).build();

        SelectTrigger selectTrigger = new SelectTrigger(emitter);

        given:
        Message msg = new Message.Builder().build()
        String SNAPSHOT_REGEX = '\\{"skipNumber":0,"%%EIO_LAST_POLL%%":"' + today + ' 00:00:00\\.\\d{3}","sqlQuery":"SELECT \\* FROM Employee WHERE JoiningDate > \'' + today + ' 00:00:00\\.\\d{3}\'","nullableResult":false\\}'

        JsonObject config = Json.createObjectBuilder().build()
        config.addProperty("sqlQuery", "SELECT * FROM Employee WHERE JoiningDate > '" + today + " 00:00:00.000'")
        config.addProperty("user", user)
        config.addProperty("password", password)
        config.addProperty("dbEngine", "mssql")
        config.addProperty("host", "")
        config.addProperty("databaseName", databaseName)
        config.addProperty("tableName", "stars")

        JsonObject snapshot = Json.createObjectBuilder().build()
        snapshot.addProperty("skipNumber", 0)

        when:
        ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
        selectTrigger.execute(params)

        then:
        0 * errorCallback.receive(_)
        1 * dataCallback.receive({
            it.toString() == '{"body":{"ID":"1","name":"Sun","radius":"50","destination":"170.0","createdat":"2018-06-14 10:00:00.0","RowNum":"1"},"attachments":{}}'
        })
        1 * snapshotCallback.receive({
            it.toString().matches(SNAPSHOT_REGEX)
        })
    }
}
