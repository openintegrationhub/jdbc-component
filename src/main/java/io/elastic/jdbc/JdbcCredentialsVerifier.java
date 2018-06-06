package io.elastic.jdbc;

import com.google.gson.JsonObject;
import io.elastic.api.CredentialsVerifier;
import io.elastic.api.InvalidCredentialsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcCredentialsVerifier implements CredentialsVerifier {

    private static final Logger logger = LoggerFactory.getLogger(JdbcCredentialsVerifier.class);

    @Override
    public void verify(JsonObject configuration) throws InvalidCredentialsException {

        logger.info("About to connect to database using given credentials");

        Connection connection = null;

        try {
            connection = Utils.getConnection(configuration);
            logger.info("Successfully connected to database. Credentials verified.");
        } catch (Exception e) {
            throw new InvalidCredentialsException("Failed to connect to database", e);
        } finally {
            if (connection != null) {
                logger.info("Closing database connection");
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to closed database connection", e);
                }
            }
        }
    }
}
