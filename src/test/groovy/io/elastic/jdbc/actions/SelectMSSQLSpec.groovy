import io.elastic.api.EventEmitter
import io.elastic.api.ExecutionParameters
import io.elastic.api.Message
import io.elastic.jdbc.actions.SelectAction
import spock.lang.*

import javax.json.Json
import javax.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

@Ignore
class SelectMSSQLSpec extends Specification {

  @Shared
  def connectionString = System.getenv("CONN_URI_MSSQL")
  @Shared
  def user = System.getenv("CONN_USER_MSSQL")
  @Shared
  def password = System.getenv("CONN_PASSWORD_MSSQL")
  @Shared
  def databaseName = System.getenv("CONN_DBNAME_MSSQL")
  @Shared
  def host = System.getenv("CONN_HOST_MSSQL")

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
  EventEmitter emitter
  @Shared
  SelectAction action

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
    emitter = new EventEmitter.Builder().onData(dataCallback).onSnapshot(snapshotCallback).onError(errorCallback).onRebound(reboundCallback).build()
    action = new SelectAction(emitter)
  }

  def runAction(JsonObject config, JsonObject body, JsonObject snapshot) {
    Message msg = new Message.Builder().body(body).build()
    ExecutionParameters params = new ExecutionParameters(msg, config, snapshot)
    action.execute(params);
  }

  def getStarsConfig() {
    JsonObject config = Json.createObjectBuilder().build();

    config.addProperty("idColumn", "id")
    config.addProperty("tableName", "stars")
    config.addProperty("user", user)
    config.addProperty("password", password)
    config.addProperty("dbEngine", "mssql")
    config.addProperty("host", host)
    config.addProperty("databaseName", databaseName)
    return config;
  }

  def prepareStarsTable() {
    String sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE stars (id int, name varchar(255) NOT NULL, date datetime, radius int, destination int)");
  }

  def getRecords(tableName) {
    ArrayList<String> records = new ArrayList<String>();
    String sql = "SELECT * FROM " + tableName;
    ResultSet rs = connection.createStatement().executeQuery(sql);
    while (rs.next()) {
      records.add(rs.toRowResult().toString());
    }
    rs.close();
    return records;
  }

  def cleanupSpec() {
    String sql = "IF OBJECT_ID('persons', 'U') IS NOT NULL\n" +
        "  DROP TABLE persons;"

    connection.createStatement().execute(sql)
    sql = "IF OBJECT_ID('stars', 'U') IS NOT NULL\n" +
        "  DROP TABLE stars;"
    connection.createStatement().execute(sql)
    connection.close()
  }

  def "one insert"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build();

    JsonObject body = Json.createObjectBuilder().build();
    body.addProperty("id", "1")
    body.addProperty("name", "Taurus")
    body.addProperty("date", "2015-02-19 10:10:10.0")
    body.addProperty("radius", "123")

    runAction(getStarsConfig(), body, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=Taurus, date=2015-02-19 10:10:10.0, radius=123, destination=null}'
  }

  def "one insert, incorrect value: string in integer field"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build();

    JsonObject body = Json.createObjectBuilder().build();
    body.addProperty("id", "1")
    body.addProperty("name", "Taurus")
    body.addProperty("radius", "test")

    String exceptionClass = "";

    try {
      runAction(getStarsConfig(), body, snapshot)
    } catch (Exception e) {
      exceptionClass = e.getClass().getName();
    }

    expect:
    exceptionClass.contains("Exception")
  }

  def "two inserts"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder().build()
    body1.addProperty("id", "1")
    body1.addProperty("name", "Taurus")
    body1.addProperty("radius", "123")

    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder().build()
    body2.addProperty("id", "2")
    body2.addProperty("name", "Eridanus")
    body2.addProperty("radius", "456")

    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 2
    records.get(0) == '{id=1, name=Taurus, date=null, radius=123, destination=null}'
    records.get(1) == '{id=2, name=Eridanus, date=null, radius=456, destination=null}'
  }

  def "one insert, one update by ID"() {

    prepareStarsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder().build()
    body1.addProperty("id", "1")
    body1.addProperty("name", "Taurus")
    body1.addProperty("radius", "123")

    runAction(getStarsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder().build()
    body2.addProperty("id", "1")
    body2.addProperty("name", "Eridanus")

    runAction(getStarsConfig(), body2, snapshot)

    ArrayList<String> records = getRecords("stars")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=Eridanus, date=null, radius=123, destination=null}'
  }


  def getPersonsConfig() {
    JsonObject config = Json.createObjectBuilder().build()
    config.addProperty("idColumn", "email")
    config.addProperty("tableName", "persons")
    config.addProperty("user", user)
    config.addProperty("password", password)
    config.addProperty("dbEngine", "mssql")
    config.addProperty("host", host)
    config.addProperty("databaseName", databaseName)
    return config;
  }

  def preparePersonsTable() {
    String sql = "IF OBJECT_ID('persons', 'U') IS NOT NULL\n" +
        "  DROP TABLE persons;"
    connection.createStatement().execute(sql);
    connection.createStatement().execute("CREATE TABLE persons (id int, name varchar(255) NOT NULL, email varchar(255) NOT NULL)");
  }

  def "one insert, name with quote"() {

    preparePersonsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder().build()
    body1.addProperty("id", "1")
    body1.addProperty("name", "O'Henry")
    body1.addProperty("email", "ohenry@elastic.io")
    runAction(getPersonsConfig(), body1, snapshot)

    ArrayList<String> records = getRecords("persons")

    expect:
    records.size() == 1
    records.get(0) == '{id=1, name=O\'Henry, email=ohenry@elastic.io}'
  }

  def "two inserts, one update by email"() {

    preparePersonsTable();

    JsonObject snapshot = Json.createObjectBuilder().build()

    JsonObject body1 = Json.createObjectBuilder().build()
    body1.addProperty("id", "1")
    body1.addProperty("name", "User1")
    body1.addProperty("email", "user1@elastic.io")
    runAction(getPersonsConfig(), body1, snapshot)

    JsonObject body2 = Json.createObjectBuilder().build()
    body2.addProperty("id", "2")
    body2.addProperty("name", "User2")
    body2.addProperty("email", "user2@elastic.io")
    runAction(getPersonsConfig(), body2, snapshot)

    JsonObject body3 = Json.createObjectBuilder().build()
    body3.addProperty("id", "3")
    body3.addProperty("name", "User3")
    body3.addProperty("email", "user2@elastic.io")
    runAction(getPersonsConfig(), body3, snapshot)

    ArrayList<String> records = getRecords("persons")

    expect:
    records.size() == 2
    records.get(0) == '{id=1, name=User1, email=user1@elastic.io}'
    records.get(1) == '{id=3, name=User3, email=user2@elastic.io}'
  }


}
