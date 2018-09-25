package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import javax.json.JsonObject;
import javax.json.JsonValue;

public class MySQL extends Query {

  public ArrayList executePolling(Connection connection) throws SQLException {
    validateQuery();
    StringBuilder sql = new StringBuilder("SELECT * FROM ");
    sql.append(tableName);
    sql.append(" WHERE ");
    sql.append(pollingField);
    sql.append(" > ?");
    if (orderField != null) {
      sql.append(" ORDER BY ").append(orderField).append(" ASC");
    }
    sql.append(" LIMIT ?");

    return getRowsExecutePolling(connection, sql.toString());
  }

  public JsonObject executeLookup(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    StringBuilder sql = new StringBuilder("SELECT * FROM ");
    sql.append(tableName);
    sql.append(" WHERE ");
    sql.append(lookupField);
    sql.append(" = ?");
    sql.append(" ORDER BY ").append(lookupField);
    sql.append(" ASC LIMIT ? OFFSET ?");
    return getLookupRow(connection, body, sql.toString(), countNumber, skipNumber);
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
      for (String key : body.keySet()) {
        Utils.setStatementParam(stmt, i, key, body);
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
      for (String key : body.keySet()) {
        Utils.setStatementParam(stmt, i, key, body);
        i++;
      }
      Utils.setStatementParam(stmt, i, idColumn, body);
      stmt.execute();
    }
  }

}
