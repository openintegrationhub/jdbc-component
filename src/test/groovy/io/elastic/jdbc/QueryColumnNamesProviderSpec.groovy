package io.elastic.jdbc

import spock.lang.*

import javax.json.Json
import javax.json.JsonObject

@Ignore
class QueryColumnNamesProviderSpec extends Specification {

  JsonObject configuration = Json.createObjectBuilder().build()
  String sqlQuery;

  def setup() {
    sqlQuery = "SELECT * FROM films WHERE watched = @watched:boolean AND created = @created:date"
  }

  def "get metadata model, given json object"() {
    configuration.addProperty("sqlQuery", sqlQuery)

    QueryColumnNamesProvider provider = new QueryColumnNamesProvider()
    JsonObject meta = provider.getMetaModel((configuration))
    print meta
    expect:
    meta.toString() == "{\"out\":{\"type\":\"object\",\"properties\":{\"watched\":{\"title\":\"watched\",\"type\":\"boolean\"},\"created\":{\"title\":\"created\",\"type\":\"date\"}}},\"in\":{\"type\":\"object\",\"properties\":{\"watched\":{\"title\":\"watched\",\"type\":\"boolean\"},\"created\":{\"title\":\"created\",\"type\":\"date\"}}}}"
  }
}
