package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import javax.json.JsonObject;
import javax.json.JsonValue;

public class MSSQL extends Query {

  public ResultSet executeSelectQuery(Connection connection, String sqlQuery, JsonObject body)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sqlQuery);
    int i = 1;
    for (Entry<String, JsonValue> entry : body.entrySet()) {
      Utils.setStatementParam(stmt, i, entry.getKey(), body);
      i++;
    }
    return stmt.executeQuery();
  }

  public ResultSet executeSelectTrigger(Connection connection, String sqlQuery)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sqlQuery);
    if (pollingValue != null) {
      stmt.setTimestamp(1, pollingValue);
    }
    return stmt.executeQuery();
  }

  public ResultSet executePolling(Connection connection) throws SQLException {
    validateQuery();
    String sql = "WITH Results_CTE AS" +
        "(" +
        "    SELECT" +
        "        *," +
        "        ROW_NUMBER() OVER (ORDER BY " + pollingField + ") AS RowNum" +
        "    FROM " + tableName +
        "    WHERE " + pollingField + " > ?" +
        " )" +
        " SELECT *" +
        " FROM Results_CTE" +
        " WHERE RowNum > ?" +
        " AND RowNum < ?";
    PreparedStatement stmt = connection.prepareStatement(sql);
    stmt.setTimestamp(1, pollingValue);
    stmt.setInt(2, skipNumber);
    stmt.setInt(3, countNumber + skipNumber);
    return stmt.executeQuery();
  }

  public ResultSet executeLookup(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "WITH Results_CTE AS" +
        "(" +
        "    SELECT" +
        "        *," +
        "        ROW_NUMBER() OVER (ORDER BY " + lookupField + ") AS RowNum" +
        "    FROM " + tableName +
        "    WHERE " + lookupField + " = ?" +
        " )" +
        " SELECT *" +
        " FROM Results_CTE" +
        " WHERE RowNum > ?" +
        " AND RowNum < ?";
    PreparedStatement stmt = connection.prepareStatement(sql);
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      Utils.setStatementParam(stmt, 1, entry.getKey(), body);
    }
    stmt.setInt(2, skipNumber);
    stmt.setInt(3, countNumber + skipNumber);
    return stmt.executeQuery();
  }

  public int executeDelete(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "DELETE" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    PreparedStatement stmt = connection.prepareStatement(sql);
    stmt.setString(1, lookupValue);
    return stmt.executeUpdate();
  }

  public boolean executeRecordExists(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT COUNT(*)" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    PreparedStatement stmt = connection.prepareStatement(sql);
    try {
      Utils.setStatementParam(stmt, 1, lookupField, body);
      ResultSet rs = stmt.executeQuery();
      rs.next();
      return rs.getInt(1) > 0;
    } finally {
      if (stmt != null) {
        stmt.close();
      }
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
    PreparedStatement stmt = connection.prepareStatement(sql);
    try {
      int i = 1;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      stmt.execute();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public void executeUpsert(Connection connection, String idColumn, JsonObject body)
      throws SQLException {
    validateQuery();

    StringBuilder keys = new StringBuilder();
    StringBuilder values = new StringBuilder();
    StringBuilder setString = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (!entry.getKey().equals(idColumn)) {
        if (setString.length() > 0) {
          setString.append(",");
        }
        setString.append(entry.getKey()).append(" = ?");
      }
      if (keys.length() > 0) {
        keys.append(",");
      }
      keys.append(entry.getKey());
      if (values.length() > 0) {
        values.append(",");
      }
      values.append("?");
    }
    String sql = "BEGIN TRANSACTION;" +
        " IF EXISTS (SELECT * FROM " + tableName +
        " WHERE " + idColumn + "= ?)" +
        " UPDATE " + tableName +
        " SET " + setString.toString() +
        " WHERE " + idColumn + " = ?" +
        " ELSE INSERT INTO " + tableName +
        " (" + keys.toString() + ")" +
        " VALUES (" + values.toString() + ")" +
        " COMMIT;";
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sql);
      Utils.setStatementParam(stmt, 1, idColumn, body);
      int i = 2;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        if (!entry.getKey().equals(idColumn)) {
          Utils.setStatementParam(stmt, i, entry.getKey(), body);
          i++;
        }
      }
      Utils.setStatementParam(stmt, i, idColumn, body);
      i++;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      stmt.execute();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
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
    PreparedStatement stmt = connection.prepareStatement(sql);
    try {
      int i = 1;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      Utils.setStatementParam(stmt, i, idColumn, body);
      stmt.execute();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }
}
