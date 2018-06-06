package io.elastic.jdbc;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class ColumnNamesProvider implements DynamicMetadataProvider, SelectModelProvider {
    private static final Logger logger = LoggerFactory.getLogger(ColumnNamesProvider.class);

    public JsonObject getSelectModel(JsonObject configuration) {
        JsonObject result = new JsonObject();
        JsonObject properties = getColumns(configuration);
        for (Map.Entry<String,JsonElement> entry : properties.entrySet()) {
            JsonObject field = entry.getValue().getAsJsonObject();
            result.addProperty(entry.getKey(), field.get("title").getAsString());
        }
        return result;
    }

    /**
     * Returns Columns list as metadata
     * @param configuration
     * @return
     */

    public JsonObject getMetaModel(JsonObject configuration) {
        JsonObject result = new JsonObject();
        JsonObject inMetadata = new JsonObject();
        JsonObject properties = getColumns(configuration);
        inMetadata.addProperty("type", "object");
        inMetadata.add("properties", properties);
        result.add("out", inMetadata);
        result.add("in", inMetadata);
        return result;
    }

    public JsonObject getColumns(JsonObject configuration) {
        if (configuration.get("tableName") == null || configuration.get("tableName").getAsString().isEmpty()) {
            throw new RuntimeException("Table name is required");
        }
        String tableName = configuration.get("tableName").getAsString();
        JsonObject properties = new JsonObject();
        Connection connection = null;
        ResultSet rs = null;
        String schemaName = null;
        Boolean isEmpty = true;
        try {
            connection = Utils.getConnection(configuration);
            DatabaseMetaData dbMetaData = connection.getMetaData();
            if (tableName.contains(".")) {
                schemaName = tableName.split("\\.")[0];
                tableName = tableName.split("\\.")[1];
            }
            rs = dbMetaData.getColumns(null, schemaName, tableName, "%");
            while (rs.next()) {
                JsonObject field = new JsonObject();
                String name = rs.getString("COLUMN_NAME");
                Boolean isRequired = rs.getInt("NULLABLE") == 0 && !rs.getString("IS_AUTOINCREMENT").equals("YES");
                field.addProperty("required", isRequired);
                field.addProperty("title", name);
                field.addProperty("type", convertType(rs.getInt("DATA_TYPE")));
                properties.add(name, field);
                isEmpty = false;
            }
            if (isEmpty) {
                properties.addProperty("", "no columns");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Failed to close result set", e.toString());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection", e.toString());
                }
            }
        }
        return properties;
    }

    /**
     * Converts JDBC column type name to js type according to http://db.apache.org/ojb/docu/guides/jdbc-types.html
     *
     * @param sqlType JDBC column type
     * @url http://db.apache.org/ojb/docu/guides/jdbc-types.html
     * @return
     */
    private String convertType(Integer sqlType) {
        if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL || sqlType == Types.TINYINT
                || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
                || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
            return "number";
        }
        if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
            return "boolean";
        }
        return "string";
    }
}
