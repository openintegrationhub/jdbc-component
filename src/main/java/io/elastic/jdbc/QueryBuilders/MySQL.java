package io.elastic.jdbc.QueryBuilders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQL extends Query {
    public ResultSet execute(Connection connection) throws SQLException {
        validateQuery();

        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(tableName);
        if (orderField != null) {
            sql.append(" ORDER BY ").append(orderField);
        }
        sql.append(" ASC LIMIT ? OFFSET ?");

        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        stmt.setInt(1, countNumber);
        stmt.setInt(2, skipNumber);
        return stmt.executeQuery();
    }
}