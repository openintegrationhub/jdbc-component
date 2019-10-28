package io.elastic.jdbc.providers;

import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemasProvider implements SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemasProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();

    LOGGER.info("Searching for %s db schemas...", configuration.getString("databaseName"));
    List<String> proceduresNames = getSchemasList(configuration);

    LOGGER.info("Found %d db schemas", proceduresNames.size());
    LOGGER.debug("Schemas: " + proceduresNames.stream().reduce((s1, s2) -> s1 + ", " + s2).orElse(""));

    proceduresNames.forEach(procedure -> result.add(procedure, procedure));

    if (configuration.getString("dbEngine").equals("mysql")) {
      LOGGER.info("Adding placeholder to MySQL schemas list...");
      formatMySQLSchemasResponse(result, configuration.getString("databaseName"));
    }

    LOGGER.info("Response building complete. Returning result...");
    return result.build();
  }

  public List<String> getSchemasList(JsonObject configuration) {
    List<String> result = new LinkedList<>();

    try (Connection conn = Utils.getConnection(configuration)) {
      DatabaseMetaData meta = conn.getMetaData();
      ResultSet res = meta.getSchemas();
      while (res.next()) {
        result.add(res.getString("TABLE_SCHEM"));
      }
      res.close();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    return result;
  }

  private JsonObjectBuilder formatMySQLSchemasResponse(JsonObjectBuilder structure, String dbName) {
    structure.add(dbName, dbName);
    return structure;
  }

}
