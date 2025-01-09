package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

public class AlloyDBEngine {

    DataSource dataSource;

    public AlloyDBEngine(
            String database,
            String user,
            String password,
            String project_id,
            String cluster,
            String region,
            String instance,
            String ipType,
            String iamAccountEmail
    ) {
        Boolean enableIAMAuth = false;
        if (user != null && !user.isBlank()) {
            if (password != null && !password.isBlank()) {
                enableIAMAuth = false;
            }
        } else {
            enableIAMAuth = true;
            if (iamAccountEmail != null && !iamAccountEmail.isBlank()) {
                user = iamAccountEmail;
            } else {
                // to be implemented
                user = getIAMPrincipalEmail();
            }
        }
        String instanceName = new StringBuilder("projects/").append(project_id).append("/locations/")
                .append(region).append("/clusters/").append(cluster).append("/instances/").append(instance).toString();
        dataSource = createDataSource(database, user, password, instanceName, ipType, enableIAMAuth);
    }

    private HikariDataSource createDataSource(
            String database,
            String user,
            String password,
            String instanceName,
            String ipType,
            Boolean enableIAMAuth
    ) {
        HikariConfig config = new HikariConfig();
        config.setUsername(ensureNotBlank(user, "user")); // e.g., "postgres"
        if (enableIAMAuth) {
            config.addDataSourceProperty("alloydbEnableIAMAuth", "true");
        } else {
            config.setPassword(ensureNotBlank(password, "password")); // e.g., "secret-password"
        }
        config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
        config.addDataSourceProperty("socketFactory", "com.google.cloud.alloydb.SocketFactory");
        config.addDataSourceProperty("alloydbInstanceName", ensureNotBlank(instanceName, "instanceName"));
        config.addDataSourceProperty("alloydbIpType", ensureNotBlank(ipType, "ipType"));

        return new HikariDataSource(config);
    }

    private String getIAMPrincipalEmail() {
        //to be implemented
        return "";
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void initVectorStoreTable() {
        //to be implemented
    }

    public void initChatHistoryTable() {
        //to be implemented
    }

    public void initDocumentTable() {
        //to be implemented
    }
}
