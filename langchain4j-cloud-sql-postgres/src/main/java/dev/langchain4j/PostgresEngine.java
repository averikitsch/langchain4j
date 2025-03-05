package dev.langchain4j;

import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

public class PostgresEngine {

  private DataSource dataSource;

  public PostgresEngine(
      String projectId,
      String region,
      String instance,
      String database,
      String user,
      String password,
      String ipType,
      String iamAccountEmail) {
    Boolean enableIAMAuth = false;
    if (user != null && !user.isBlank() && password != null && !password.isBlank()) {
      enableIAMAuth = false;
    } else {
      enableIAMAuth = true;
      if (iamAccountEmail != null && !iamAccountEmail.isBlank()) {
        user = iamAccountEmail;
      } else {
        // to be implemented
        user = getIAMPrincipalEmail();
      }
    }
    String instanceName =
        new StringBuilder(ensureNotBlank(projectId, "projectId"))
            .append(":")
            .append(ensureNotBlank(region, "region"))
            .append(":")
            .append(ensureNotBlank(instance, "instance"))
            .toString();
    dataSource = createDataSource(database, user, password, instanceName, ipType, enableIAMAuth);
  }

  private HikariDataSource createDataSource(
      String database,
      String user,
      String password,
      String instanceName,
      String ipType,
      Boolean enableIAMAuth) {
    HikariConfig config = new HikariConfig();
    config.setUsername(ensureNotBlank(user, "user"));
    if (enableIAMAuth) {
      config.addDataSourceProperty("enableIAMAuth", "true");
    } else {
      config.setPassword(ensureNotBlank(password, "password"));
    }
    config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
    config.addDataSourceProperty("socketFactory", "com.google.cloud.postgres.SocketFactory");
    config.addDataSourceProperty("cloudSqlInstance", ensureNotBlank(instanceName, "instanceName"));
    config.addDataSourceProperty("ipType", ensureNotBlank(ipType, "ipType"));

    return new HikariDataSource(config);
  }

  private String getIAMPrincipalEmail() {
    // to be implemented
    return "";
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private String projectId;
    private String region;
    private String instance;
    private String database;
    private String user;
    private String password;
    private String ipType;
    private String iamAccountEmail;

    public Builder() {}

    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder region(String region) {
      this.region = region;
      return this;
    }

    public Builder instance(String instance) {
      this.instance = instance;
      return this;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder ipType(String ipType) {
      this.ipType = ipType;
      return this;
    }

    public Builder iamAccountEmail(String iamAccountEmail) {
      this.iamAccountEmail = iamAccountEmail;
      return this;
    }

    PostgresEngine build() {
      return new PostgresEngine(
          projectId, region, instance, database, user, password, ipType, iamAccountEmail);
    }
  }
}
