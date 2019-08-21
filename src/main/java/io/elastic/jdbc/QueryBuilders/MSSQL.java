package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.ProcedureFieldsNameProvider;
import io.elastic.jdbc.ProcedureParameter;
import io.elastic.jdbc.ProcedureParameter.Direction;
import io.elastic.jdbc.Utils;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public class MSSQL extends Query {

  public ArrayList executePolling(Connection connection) throws SQLException {
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
        " WHERE RowNum <= ?";
    return getRowsExecutePolling(connection, sql);
  }

  public JsonObject executeLookup(Connection connection, JsonObject body) throws SQLException {
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
    return getLookupRow(connection, body, sql, skipNumber, countNumber + skipNumber);
  }

  public int executeDelete(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
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

  @Override
  protected CallableStatement prepareCallableStatement(Connection connection, String procedureName,
      Map<String, ProcedureParameter> procedureParams, JsonObject messageBody)
      throws SQLException {
    CallableStatement stmt = connection.prepareCall(
        String.format("{call %s%s}", procedureName,
            generateStatementWildcardMask(procedureParams)));

    for (int inc = 1; inc <= procedureParams.size(); inc++) {
      final int order = inc;
      ProcedureParameter parameter = procedureParams.values()
          .stream()
          .filter(p -> p.getOrder() == order)
          .findFirst().orElseThrow(() -> new IllegalStateException("Can't find parameter by order"));

      if (parameter.getDirection() == Direction.IN || parameter.getDirection() == Direction.INOUT) {
        if (parameter.getDirection() == Direction.INOUT) {
          stmt.registerOutParameter(inc, parameter.getType());
        }

        String type = Utils.cleanJsonType(Utils.detectColumnType(parameter.getType(), ""));
        switch (type) {
          case ("number"):
            stmt.setObject(inc,
                messageBody.getJsonNumber(parameter.getName()).toString(),
                parameter.getType());
            break;
          case ("boolean"):
            stmt.setObject(inc, messageBody.getBoolean(parameter.getName()),
                parameter.getType());
            break;
          default:
            stmt.setObject(inc, messageBody.getString(parameter.getName()),
                parameter.getType());
        }
      } else if (parameter.getDirection() == Direction.OUT) {
        stmt.registerOutParameter(inc, parameter.getType());
      }
    }

    return stmt;
  }

  public JsonObject callProcedure(Connection connection, JsonObject body, JsonObject configuration)
      throws SQLException {

    Map<String, ProcedureParameter> procedureParams = ProcedureFieldsNameProvider
        .getProcedureMetadata(configuration).stream()
        .collect(Collectors.toMap(ProcedureParameter::getName, Function.identity()));

    CallableStatement stmt = prepareCallableStatement(connection,
        configuration.getString("procedureName"), procedureParams, body);

    stmt.execute();

    JsonObjectBuilder resultBuilder = Json.createObjectBuilder();

    procedureParams.values().stream()
        .filter(param -> param.getDirection() == Direction.OUT
            || param.getDirection() == Direction.INOUT)
        .forEach(param -> {
          try {
            addValueToResultJson(resultBuilder, stmt, procedureParams, param.getName());
          } catch (SQLException e) {
            e.printStackTrace();
          }
        });

    stmt.close();

    return resultBuilder.build();
  }

}
