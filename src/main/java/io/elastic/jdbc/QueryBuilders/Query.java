package io.elastic.jdbc.QueryBuilders;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Query {
    protected Integer skipNumber = 0;
    protected Integer countNumber = 5000;
    protected String tableName = null;
    protected String orderField = null;

    public Query skip(Integer skip) {
        this.skipNumber = skip;
        return this;
    }

    public Query count(Integer count) {
        this.countNumber = count;
        return this;
    }

    public Query from(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public Query orderBy(String fieldName) {
        this.orderField = fieldName;
        return this;
    }

    abstract public ResultSet execute(Connection connection) throws SQLException;

    public void validateQuery() {
        if (tableName == null) {
            throw new RuntimeException("Table name is required field");
        }
        if (orderField == null) {
            throw new RuntimeException("Order field is required field");
        }
    }
}
