package dev.langchain4j.data.document.loader.alloydb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.engine.AlloyDBEngine;

/**
 * Loads data from Alloy DB.
 * <br>
 * The data in differeent formats is returned in form of {@link Document}.
 *
 */
public class AlloyDBLoader {

    private final String query;
    private final List<String> contentColumns;
    private final List<String> metadataColumns;
    private final BiFunction<Map<String, Object>, List<String>, String> formatter;
    private final String metadataJsonColumn;
    private final Connection connection;
    private static final Logger log = LoggerFactory.getLogger(AlloyDBEngine.class.getName());
    private static final String DEFAULT_METADATA_COL = "langchain_metadata";

    private AlloyDBLoader(Connection connection, String query,
            BiFunction<Map<String, Object>, List<String>, String> formatter, List<String> contentColumns,
            List<String> metadataColumns, String metadataJsonColumn) {
        this.connection = connection;
        this.query = query;
        this.formatter = formatter;
        this.contentColumns = contentColumns;
        this.metadataColumns = metadataColumns;
        this.metadataJsonColumn = metadataJsonColumn;
    }

    public static class Builder {

        private final AlloyDBEngine engine;
        private Connection connection;
        private String tableName;
        private String query;
        private String metadataJsonColumn;
        private String schemaName = "public";
        private List<String> contentColumns = new ArrayList<>();
        private List<String> metadataColumns = new ArrayList<>();
        private String format;
        private BiFunction<Map<String, Object>, List<String>, String> formatter;

        public Builder(AlloyDBEngine engine) {
            this.engine = engine;
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder formatter(BiFunction<Map<String, Object>, List<String>, String> formatter) {
            if (this.format != null && formatter != null) {
                throw new IllegalArgumentException("Only one of 'format' or 'formatter' should be specified.");
            }
            this.formatter = formatter;
            return this;
        }

        public Builder format(String format) {
            if (format != null && this.formatter != null) {
                throw new IllegalArgumentException("Only one of 'format' or 'formatter' should be specified.");
            }
            switch (format) {
                case "csv":
                    this.formatter = AlloyDBLoader::csvFormatter;
                    break;
                case "text":
                    this.formatter = AlloyDBLoader::textFormatter;
                    break;
                case "JSON":
                    this.formatter = AlloyDBLoader::jsonFormatter;
                    break;
                case "YAML":
                    this.formatter = AlloyDBLoader::yamlFormatter;
                    break;
                default:
                    this.formatter = AlloyDBLoader::textFormatter;
            }
            return this;
        }

        public Builder contentColumns(List<String> contentColumns) {
            this.contentColumns = contentColumns;
            return this;
        }

        public Builder metadataColumns(List<String> metadataColumns) {
            this.metadataColumns = metadataColumns;
            return this;
        }

        public Builder metadataJsonColumn(String metadataJsonColumn) {
            this.metadataJsonColumn = metadataJsonColumn;
            return this;
        }

        public AlloyDBLoader build() throws SQLException {
            if ((this.query == null || this.query.isEmpty()) && (this.tableName == null || this.tableName.isEmpty())) {
                throw new IllegalArgumentException("Either query or tableName must be specified.");
            }
            if (query == null) {
                query = String.format("SELECT * FROM \"%s\".\"%s\" LIMIT 1", schemaName, tableName);
            }

            List<String> columnNames = new ArrayList<>();
            try (Connection pool = engine.getConnection(); PreparedStatement statement = pool.prepareStatement(query); ResultSet resultSet = statement.executeQuery()) {
                this.connection = pool;
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    columnNames.add(resultSet.getMetaData().getColumnName(i));
                }
            }

            contentColumns = contentColumns == null || contentColumns.isEmpty() ? List.of(columnNames.get(0)) : contentColumns;
            metadataColumns = metadataColumns == null ? new ArrayList<>() : metadataColumns;
            for (String col : columnNames) {
                if (!contentColumns.contains(col) && !metadataColumns.contains(col)) {
                    metadataColumns.add(col);
                }
            }

            if (metadataJsonColumn != null && !columnNames.contains(metadataJsonColumn)) {
                throw new IllegalArgumentException(String.format("Column %s not found in query result %s.", metadataJsonColumn, columnNames));
            }
            if (metadataJsonColumn == null && columnNames.contains(DEFAULT_METADATA_COL)) {
                metadataJsonColumn = DEFAULT_METADATA_COL;
            } else {
                metadataJsonColumn = null;
            }

            List<String> allNames = new ArrayList<>(contentColumns);
            allNames.addAll(metadataColumns);
            for (String name : allNames) {
                if (!columnNames.contains(name)) {
                    throw new IllegalArgumentException(String.format("Column %s not found in query result %s.", name, columnNames));
                }
            }
            return new AlloyDBLoader(connection, query, formatter, contentColumns, metadataColumns, metadataJsonColumn);
        }
    }

    private static String textFormatter(Map<String, Object> row, List<String> contentColumns) {
        StringBuilder sb = new StringBuilder();
        for (String column : contentColumns) {
            if (row.containsKey(column)) {
                sb.append(row.get(column)).append(" ");
            }
        }
        return sb.toString().trim();
    }

    private static String csvFormatter(Map<String, Object> row, List<String> contentColumns) {
        StringBuilder sb = new StringBuilder();
        for (String column : contentColumns) {
            if (row.containsKey(column)) {
                sb.append(row.get(column)).append(", ");
            }
        }
        return sb.toString().trim().replaceAll(", $", ""); // Remove trailing comma
    }

    private static String yamlFormatter(Map<String, Object> row, List<String> contentColumns) {
        StringBuilder sb = new StringBuilder();
        for (String column : contentColumns) {
            if (row.containsKey(column)) {
                sb.append(column).append(": ").append(row.get(column)).append("\n");
            }
        }
        return sb.toString().trim();
    }

    private static String jsonFormatter(Map<String, Object> row, List<String> contentColumns) {
        JsonObject json = new JsonObject();
        for (String column : contentColumns) {
            if (row.containsKey(column)) {
                json.addProperty(column, (String) row.get(column));
            }
        }
        return json.toString();
    }

    /**
     * Loads data from Alloy Db in form of {@code Document}.
     *
     * @return List<Document> list of documents
     * @throws SQLException if databse error occurs
     */
    public List<Document> load() throws SQLException {
        List<Document> documents = new ArrayList<>();
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            Map<String, Object> rowData = new HashMap<>();
            for (String column : contentColumns) {
                rowData.put(column, resultSet.getObject(column));
            }
            for (String column : metadataColumns) {
                rowData.put(column, resultSet.getObject(column));
            }
            if (metadataJsonColumn != null) {
                rowData.put(metadataJsonColumn, resultSet.getObject(metadataJsonColumn));
            }

            Document doc = parseDocFromRow(rowData);
            documents.add(doc);
        }

        return documents;
    }

    private Document parseDocFromRow(Map<String, Object> row) {
        String pageContent = formatter.apply(row, contentColumns);
        Metadata metadata = new Metadata();

        if (metadataJsonColumn != null && row.containsKey(metadataJsonColumn)) {
            try {
                JsonObject jsonMetadata = JsonParser.parseString(
                        row.get(metadataJsonColumn).toString()).getAsJsonObject();
                for (String key : jsonMetadata.keySet()) {
                    metadata.put(key, jsonMetadata.get(key).toString());
                }
            } catch (JsonSyntaxException e) { // Handle JSON parsing errors
                log.debug(e.getMessage(), e.getCause());
            }
        }

        for (String column : metadataColumns) {
            if (row.containsKey(column) && !column.equals(metadataJsonColumn)) {
                metadata.put(column, row.get(column).toString());
            }
        }

        return new Document(pageContent, metadata);
    }
}
