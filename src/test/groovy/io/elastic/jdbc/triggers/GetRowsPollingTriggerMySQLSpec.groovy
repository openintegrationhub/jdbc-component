package io.elastic.jdbc.triggers

import io.elastic.api.EventEmitter
import io.elastic.api.EventEmitter.Callback
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import spock.lang.*

import javax.json.JsonObjectBuilder
import java.sql.Connection
import java.sql.DriverManager

@Ignore
class GetRowsPollingTriggerMySQLSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_MYSQL")
  @Shared
  def user = System.getenv("CONN_USER_MYSQL")
  @Shared
  def password = System.getenv("CONN_PASSWORD_MYSQL")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_MYSQL")
  @Shared
  def host = System.getenv("CONN_HOST_MYSQL")

  @Shared
  Connection connection;

  def setup() {
    connection = DriverManager.getConnection(connectionString, user, password)

    String sql = "DROP TABLE IF EXISTS stars"
    connection.createStatement().execute(sql)

    sql = "CREATE TABLE stars (id int, isDead boolean, name varchar(255) NOT NULL, radius int, destination float, createdat timestamp)"
    connection.createStatement().execute(sql)

    sql = "INSERT INTO stars (id, isDead, name, radius, destination, createdat) VALUES (1, false, 'Sun', 50, 170, '2018-06-14 10:00:00')"
    connection.createStatement().execute(sql)
  }

  def cleanupSpec() {
    connection.createStatement().execute("DROP TABLE stars")
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

    GetRowsPollingTrigger getRowsPollingTrigger = new GetRowsPollingTrigger(emitter);

    given:
    Message msg = new Message.Builder().build();

    JsonObjectBuilder config = Json.createObjectBuilder()
    config.add("pollingField", "createdat")
        .add("pollingValue", "2018-06-14 00:00:00")
        .add("user", user)
        .add("password", password)
        .add("dbEngine", "mysql")
        .add("host", host)
        .add("databaseName", databaseName)
        .add("tableName", "stars")

    JsonObjectBuilder snapshot = Json.createObjectBuilder()
    snapshot.add("skipNumber", 0)

    when:
    ExecutionParameters params = new ExecutionParameters(msg, config.build(), snapshot.build())
    getRowsPollingTrigger.execute(params)

    then:
    0 * errorCallback.receive(_)
    1 * dataCallback.receive({
      it.toString() == '{"body":{"id":"1","isDead":"0","name":"Sun","radius":"50","destination":"170.0","createdat":"2018-06-14 13:00:00.0"},"attachments":{}}'
    })
    1 * snapshotCallback.receive({
      it.toString() == '{"skipNumber":1,"tableName":"stars","pollingField":"createdat","pollingValue":"2018-06-14 10:00:00.000"}'
    })
  }
}