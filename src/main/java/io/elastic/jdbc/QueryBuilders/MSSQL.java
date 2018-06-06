package io.elastic.jdbc.QueryBuilders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MSSQL extends Query {
    public ResultSet execute(Connection connection) throws SQLException {
        validateQuery();
        String sql = "WITH Results_CTE AS" +
                "(" +
                "    SELECT" +
                "        *," +
                "        ROW_NUMBER() OVER (ORDER BY " + orderField + ") AS RowNum" +
                "    FROM " + tableName +
                " )" +
                " SELECT *" +
                " FROM Results_CTE" +
                " WHERE RowNum > ?" +
                " AND RowNum < ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, skipNumber);
        stmt.setInt(2, countNumber + skipNumber);
        return stmt.executeQuery();
    }
}