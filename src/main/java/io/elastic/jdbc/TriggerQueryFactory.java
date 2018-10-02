package io.elastic.jdbc;

import io.elastic.jdbc.QueryBuilders.MSSQLOld;
import io.elastic.jdbc.QueryBuilders.MySQLOld;
import io.elastic.jdbc.QueryBuilders.OracleOld;
import io.elastic.jdbc.QueryBuilders.QueryOld;

public class TriggerQueryFactory {
    public QueryOld getQuery(String dbEngine) {
        if (dbEngine.toLowerCase().equals("oracle")) {
            return new OracleOld();
        }
        if (dbEngine.toLowerCase().equals("mssql")) {
            return new MSSQLOld();
        }
        return new MySQLOld();
    }
}
