package io.elastic.jdbc.actions.procedure

import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.ExecuteStoredProcedure
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class ExecuteStoredProcedureMysqlSpec extends Specification {
    @Shared
    def user = System.getenv("CONN_USER_MYSQL")
    @Shared
    def password = System.getenv("CONN_USER_MYSQL")
    @Shared
    def databaseName = System.getenv("CONN_USER_MYSQL")
    @Shared
    def host = System.getenv("CONN_USER_MYSQL")
    @Shared
    def port = System.getenv("CONN_USER_MYSQL")
    @Shared
    def connectionString = "jdbc:mysql://" + host + ":" + port + "/" + databaseName

    @Shared
    Connection connection

    @Shared
    EventEmitter.Callback errorCallback
    @Shared
    EventEmitter.Callback snapshotCallback
    @Shared
    EventEmitter.Callback dataCallback
    @Shared
    EventEmitter.Callback reboundCallback
    @Shared
    EventEmitter.Callback httpReplyCallback
    @Shared
    EventEmitter emitter
    @Shared
    ExecuteStoredProcedure action

    def setupSpec() {
        connection = DriverManager.getConnection(connectionString, user, password)
    }

    def setup() {
        createAction()
    }

    def createAction() {
        errorCallback = Mock(EventEmitter.Callback)
        snapshotCallback = Mock(EventEmitter.Callback)
        dataCallback = Mock(EventEmitter.Callback)
        reboundCallback = Mock(EventEmitter.Callback)
        httpReplyCallback = Mock(EventEmitter.Callback)
        emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback)
                .onRebound(reboundCallback).onHttpReplyCallback(httpReplyCallback).build()
        action = new ExecuteStoredProcedure()
    }

    def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
        Message msg = new Message.Builder().body(body).build()
        ExecutionParameters params = new ExecutionParameters(msg, emitter, config, snapshot)
        action.execute(params);
    }

    def getStarsConfig() {
        JsonObject config = Json.createObjectBuilder()
                .add("schemaName", "ELASTICIO")
                .add("procedureName", "GET_CUSTOMER_BY_ID_AND_NAME")
                .add("user", user)
                .add("password", password)
                .add("dbEngine", "mysql")
                .add("host", host)
                .add("port", port)
                .add("databaseName", databaseName)
                .build();
        return config;
    }

    def prepareStarsTable() {
        connection.createStatement().execute("DROP TABLE IF EXISTS CUSTOMERS")
        connection.createStatement().execute("DROP PROCEDURE IF EXISTS GET_CUSTOMER_BY_ID_AND_NAME")

        connection.createStatement().execute("create table CUSTOMERS (\n" +
                "PID INT NOT NULL,\n" +
                "NAME varchar(128),\n" +
                "CITY varchar(256),\n" +
                "JOINDATE DATETIME(6))");


        connection.createStatement().execute("create procedure GET_CUSTOMER_BY_ID_AND_NAME(\n" +
                "IN p_cus_id int,\n" +
                "INOUT o_name varchar(128),\n" +
                "OUT o_city varchar(256),\n" +
                "OUT o_date datetime(6))\n" +
                "BEGIN\n" +
                "  SELECT NAME, CITY, JOINDATE\n" +
                "  INTO o_name, o_city, o_date\n" +
                "  from  CUSTOMERS\n" +
                "  WHERE PID = p_cus_id\n" +
                "  AND NAME = o_name;\n" +
                "END;");

        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (1,'Alice','Kyiv',STR_TO_DATE('2019-07-09T09:20:30Z', '%Y-%m-%dT%TZ'))")
        connection.createStatement().execute("INSERT INTO CUSTOMERS (PID,NAME,CITY,JOINDATE) VALUES (2,'Bob','London',STR_TO_DATE('2019-07-09T09:20:31Z', '%Y-%m-%dT%TZ'))")
    }

    def cleanupSpec() {
        connection.createStatement().execute("DROP TABLE IF EXISTS GET_CUSTOMER_BY_ID_AND_NAME")
        connection.createStatement().execute("DROP PROCEDURE IF EXISTS GET_CUSTOMER_BY_ID_AND_NAME")
        connection.close()
    }

    def "call procedure"() {

        prepareStarsTable();

        JsonObject snapshot = Json.createObjectBuilder().build()

        JsonObject body = Json.createObjectBuilder()
                .add("p_cus_id", 2)
                .add("o_name", "Bob")
                .build();

        when:
        runAction(getStarsConfig(), body, snapshot)
        then:
        1 * dataCallback.receive(_)
        0 * errorCallback.receive(_)
    }
}