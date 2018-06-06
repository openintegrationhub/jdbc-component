package io.elastic.jdbc;

import io.elastic.jdbc.QueryBuilders.MSSQL;
import io.elastic.jdbc.QueryBuilders.MySQL;
import io.elastic.jdbc.QueryBuilders.Oracle;
import io.elastic.jdbc.QueryBuilders.Query;

public class TriggerQueryFactory {
    public Query getQuery(String dbEngine) {
        if (dbEngine.toLowerCase().equals("oracle")) {
            return new Oracle();
        }
        if (dbEngine.toLowerCase().equals("mssql")) {
            return new MSSQL();
        }
        return new MySQL();
    }
}
