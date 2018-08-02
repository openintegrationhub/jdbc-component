package io.elastic.jdbc.actions;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class UpsertRowByPrimaryKey implements Module {

  private static final Logger logger = LoggerFactory.getLogger(UpsertRowByPrimaryKey.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String PROPERTY_TABLE_NAME = "tableName";
  private static final String PROPERTY_ID_COLUMN = "idColumn";

  private Connection connection = null;
  /* TODO: UpsertRowByPrimaryKey action */
  @Override
  public void execute(ExecutionParameters parameters) {
    /*
    final JsonObject configuration = parameters.getConfiguration();
    final JsonObject body = parameters.getMessage().getBody();
    if (!configuration.has(PROPERTY_TABLE_NAME) || configuration.get(PROPERTY_TABLE_NAME)
        .isJsonNull() || configuration.get(PROPERTY_TABLE_NAME).getAsString().isEmpty()) {
      throw new RuntimeException("Table name is required field");
    }
    if (!configuration.has(PROPERTY_ID_COLUMN) || configuration.get(PROPERTY_ID_COLUMN).isJsonNull()
        || configuration.get("idColumn").getAsString().isEmpty()) {
      throw new RuntimeException("ID column is required field");
    }
    String tableName = configuration.get(PROPERTY_TABLE_NAME).getAsString();
    String idColumn = configuration.get(PROPERTY_ID_COLUMN).getAsString();
    String idColumnValue = null;
    if (!(!body.has(idColumn) || body.get(idColumn).isJsonNull() || body.get(idColumn).getAsString()
        .isEmpty())) {
      idColumnValue = body.get(idColumn).getAsString();
    }
    logger.info("ID column value: {}", idColumnValue);
    String db = configuration.get(PROPERTY_DB_ENGINE).getAsString();
    boolean isOracle = db.equals(Engines.ORACLE.name().toLowerCase());
    try {
      connection = Utils.getConnection(configuration);
      Map<String, String> columnTypes = Utils.getColumnTypes(connection, isOracle, tableName);
      logger.info("Detected column types: " + columnTypes);
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(PROPERTY_DB_ENGINE);
      query.from(tableName).lookup(idColumn, idColumnValue);
      if (query.executeRecordExists(connection)) {
        query.executeUpdate(connection, tableName, idColumn, idColumnValue, body);
      } else {
        query.executeInsert(connection, tableName, body);
      }
      this.getEventEmitter().emitData(new Message.Builder().body(body).build());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          logger.error(e.toString());
        }
      }
    }
    */
  }

}