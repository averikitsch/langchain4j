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
    private static final String DEFAULT_CONTENT_COL = "page_content";
    private static final String DEFAULT_METADATA_COL = "langchain_metadata";
    private static final Logger log = LoggerFactory.getLogger(AlloyDBEngine.class.getName());

    private AlloyDBLoader(Connection connection, String query, List<String> contentColumns, List<String> metadataColumns,
            BiFunction<Map<String, Object>, List<String>, String> formatter, String metadataJsonColumn) {
        this.connection = connection;
        this.query = query;
        this.contentColumns = contentColumns;
        this.metadataColumns = metadataColumns;
        this.formatter = formatter;
        this.metadataJsonColumn = metadataJsonColumn;
    }

    public static AlloyDBLoader create(AlloyDBEngine engine, String query, String tableName, String schemaName,
            List<String> contentColumns, List<String> metadataColumns, String metadataJsonColumn,
            String format, BiFunction<Map<String, Object>, List<String>, String> formatter) throws SQLException {

        if (tableName != null && query != null) {
            throw new IllegalArgumentException("Only one of 'tableName' or 'query' should be specified.");
        }
        if (tableName == null && query == null) {
            throw new IllegalArgumentException("At least one of the parameters 'tableName' or 'query' needs to be provided");
        }
        if (format != null && formatter != null) {
            throw new IllegalArgumentException("Only one of 'format' or 'formatter' should be specified.");
        }

        if (format != null && !format.equals("csv") && !format.equals("text") && !format.equals("JSON") && !format.equals("YAML")) {
            throw new IllegalArgumentException("format must be type: 'csv', 'text', 'JSON', 'YAML'");
        }

        if (formatter == null) {
            formatter = switch (format) {
                case "csv" ->
                    AlloyDBLoader::csvFormatter;
                case "YAML" ->
                    AlloyDBLoader::yamlFormatter;
                case "JSON" ->
                    AlloyDBLoader::jsonFormatter;
                default ->
                    AlloyDBLoader::textFormatter;
            };
        }

        if (query == null) {
            query = String.format("SELECT * FROM \"%s\".\"%s\"", schemaName == null ? "public" : schemaName, tableName);
        }

        List<String> columnNames = new ArrayList<>();
        Connection connection = engine.getConnection();
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            columnNames.add(resultSet.getMetaData().getColumnName(i));
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
        if (metadataJsonColumn != null) {
            allNames.add(metadataJsonColumn);
        }

        for (String name : allNames) {
            if (!columnNames.contains(name)) {
                throw new IllegalArgumentException(String.format("Column %s not found in query result %s.", name, columnNames));
            }
        }

        return new AlloyDBLoader(connection, query, contentColumns, metadataColumns, formatter, metadataJsonColumn);
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
