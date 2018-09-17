package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import javax.json.JsonObject;
import javax.json.JsonValue;

public class Oracle extends Query {

  public ArrayList executePolling(Connection connection) throws SQLException {
    validateQuery();
    String sql = String.format("SELECT * FROM ("
            + "SELECT ROW_NUMBER() OVER( ORDER BY %s) as rn, o.* from %s o  WHERE %s > ?) "
            + "WHERE rn<=? ORDER BY %s",
        pollingField,
        tableName,
        pollingField,
        pollingField);
    return getRowsExecutePolling(connection, sql);
  }

  public JsonObject executeLookup(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT * FROM " +
        "(SELECT  b.*, rank() OVER (ORDER BY " + lookupField + ") AS rnk FROM " +
        tableName + " b) WHERE " + lookupField + " = ? " +
        "AND rnk BETWEEN ? AND ? " +
        "ORDER BY " + lookupField;
    return getLookupRow(connection, body, sql, skipNumber, countNumber);
  }

  public int executeDelete(Connection connection, JsonObject body) throws SQLException {
    String sql = "DELETE" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, lookupValue);
      return stmt.executeUpdate();
    }
  }

  public void executeInsert(Connection connection, String tableName, JsonObject body)
      throws SQLException {
    validateQuery();
    StringBuilder keys = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (keys.length() > 0) {
        keys.append(",");
      }
      keys.append(entry.getKey());
      if (values.length() > 0) {
        values.append(",");
      }
      values.append("?");
    }
    String sql = "INSERT INTO " + tableName +
        " (" + keys.toString() + ")" +
        " VALUES (" + values.toString() + ")";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      int i = 1;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      stmt.execute();
    }
  }

  public void executeUpdate(Connection connection, String tableName, String idColumn,
      String idValue, JsonObject body) throws SQLException {
    validateQuery();
    StringBuilder setString = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (setString.length() > 0) {
        setString.append(",");
      }
      setString.append(entry.getKey()).append(" = ?");
    }
    String sql = "UPDATE " + tableName +
        " SET " + setString.toString() +
        " WHERE " + idColumn + " = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      int i = 1;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      Utils.setStatementParam(stmt, i, idColumn, body);
      stmt.execute();
    }
  }

}
