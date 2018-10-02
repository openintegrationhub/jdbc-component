package io.elastic.jdbc.triggers;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectTrigger implements Module {

  private static final Logger LOGGER = LoggerFactory.getLogger(SelectTrigger.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String LAST_POLL_PLACEHOLDER = "%%EIO_LAST_POLL%%";
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_POLLING_VALUE = "pollingValue";
  private static final String PROPERTY_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.sss";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";
  private static final String DATETIME_REGEX = "(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{1,3}))?";

  @Override
  public final void execute(ExecutionParameters parameters) {
    LOGGER.info("About to execute select trigger");
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    checkConfig(configuration);
    Connection connection = Utils.getConnection(configuration);
    Integer skipNumber = 0;

    Calendar cDate = Calendar.getInstance();
    cDate.set(cDate.get(Calendar.YEAR), cDate.get(Calendar.MONTH), cDate.get(Calendar.DATE), 0, 0,
        0);
    String dbEngine = configuration.getString(PROPERTY_DB_ENGINE);
    Timestamp pollingValue;
    Timestamp cts = new java.sql.Timestamp(cDate.getTimeInMillis());

    String formattedDate = new SimpleDateFormat(PROPERTY_DATETIME_FORMAT).format(cts);
    if (configuration.containsKey(PROPERTY_POLLING_VALUE) && Utils
        .getNonNullString(configuration, PROPERTY_POLLING_VALUE).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(configuration.getString(PROPERTY_POLLING_VALUE));
    } else if (snapshot.containsKey(PROPERTY_POLLING_VALUE) && Utils
        .getNonNullString(snapshot, LAST_POLL_PLACEHOLDER).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(snapshot.getString(LAST_POLL_PLACEHOLDER));
    } else {
      LOGGER.info(
          "There is an empty value for Start Polling From at the config and snapshot. So, we set Current Date = "
              + formattedDate);
      pollingValue = cts;
    }
    LOGGER.info("EIO_LAST_POLL = {}", pollingValue);
    String sqlQuery = configuration.getString(SQL_QUERY_VALUE);
    if (snapshot.get(PROPERTY_SKIP_NUMBER) != null) {
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);
    }
    LOGGER.info("SQL QUERY {} : ", sqlQuery);
    ResultSet rs = null;
    LOGGER.info("Executing select trigger");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      sqlQuery = Query.preProcessSelect(sqlQuery);
      if (sqlQuery.contains(LAST_POLL_PLACEHOLDER)) {
        sqlQuery = sqlQuery.replace(LAST_POLL_PLACEHOLDER, "?");
        query.selectPolling(sqlQuery, pollingValue);
      }
      LOGGER.info("SQL Query: {}", sqlQuery);
      ArrayList<JsonObject> resultList = query.executeSelectTrigger(connection, sqlQuery);
      for (int i = 0; i < resultList.size(); i++) {
        LOGGER.info("Columns count: {} from {}", i + 1, resultList.size());
        LOGGER.info("Emitting data {}", resultList.get(i).toString());
        parameters.getEventEmitter()
            .emitData(new Message.Builder().body(resultList.get(i)).build());
      }

      snapshot = Json.createObjectBuilder()
          .add(PROPERTY_SKIP_NUMBER, skipNumber + resultList.size())
          .add(LAST_POLL_PLACEHOLDER, pollingValue.toString())
          .add(SQL_QUERY_VALUE, sqlQuery).build();
      LOGGER.info("Emitting new snapshot {}", snapshot.toString());
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      LOGGER.error("Failed to make request", e.toString());
      throw new RuntimeException(e);
    }
  }

  private void checkConfig(JsonObject config) {
    final JsonString sqlQuery = config.getJsonString(SQL_QUERY_VALUE);

    if (sqlQuery == null || sqlQuery.toString().isEmpty()) {
      throw new RuntimeException("SQL Query is required field");
    }
  }

}
