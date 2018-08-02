package io.elastic.jdbc;

import io.elastic.jdbc.QueryBuilders.*;

public class QueryFactory {

  public Query getQuery(String dbEngine) {
    if (dbEngine.toLowerCase().equals("oracle")) {
      return new Oracle();
    }
    if (dbEngine.toLowerCase().equals("mssql")) {
      return new MSSQL();
    }
    if (dbEngine.toLowerCase().equals("postgresql")) {
      return new PostgreSQL();
    }
    if (dbEngine.toLowerCase().equals("mysql")) {
      return new MySQL();
    }
    return null;
  }
}
