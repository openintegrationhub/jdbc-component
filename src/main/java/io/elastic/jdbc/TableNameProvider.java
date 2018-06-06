package io.elastic.jdbc;

import com.google.gson.JsonObject;
import io.elastic.api.SelectModelProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableNameProvider implements SelectModelProvider {

    private static final Logger logger = LoggerFactory.getLogger(TableNameProvider.class);

    public JsonObject getSelectModel(JsonObject configuration) {
        logger.info("About to retrieve table name");

        JsonObject result = new JsonObject();
        Connection connection = null;
        ResultSet rs = null;

        try {
            connection = Utils.getConnection(configuration);
            logger.info("Successfully connected to DB");

            // get metadata
            DatabaseMetaData md = connection.getMetaData();

            // get table names
            String[] types = {"TABLE", "VIEW"};
            rs = md.getTables(null, "%", "%", types);

            // put table names to result
            String tableName;
            String schemaName;
            Boolean isEmpty = true;

            while (rs.next()) {
                tableName = rs.getString("TABLE_NAME");
                schemaName = rs.getString("TABLE_SCHEM");
                if (configuration.get("dbEngine").getAsString().toLowerCase().equals("oracle")
                        && isOracleServiceSchema(schemaName)) {
                    continue;
                }
                if (schemaName != null) {
                    tableName = schemaName + "." + tableName;
                }
                result.addProperty(tableName, tableName);
                isEmpty = false;
            }
            if (isEmpty) {
                result.addProperty("", "no tables");
            }
        } catch (SQLException e) {
            logger.error("Unexpected error", e);
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.toString());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.toString());
                }
            }
        }
        return result;
    }

    private boolean isOracleServiceSchema(String schema) {
        List<String> schemas = Arrays.asList("APPQOSSYS", "CTXSYS", "DBSNMP", "DIP", "OUTLN", "RDSADMIN", "SYS", "SYSTEM");
        return schemas.indexOf(schema) > -1;
    }
}
