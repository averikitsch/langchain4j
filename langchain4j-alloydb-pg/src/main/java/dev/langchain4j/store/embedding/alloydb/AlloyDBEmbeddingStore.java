package dev.langchain4j.store.embedding.alloydb;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.singletonList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.AlloyDBEngine;
import static dev.langchain4j.internal.Utils.isNotNullOrEmpty;
import static dev.langchain4j.internal.Utils.randomUUID;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.index.DistanceStrategy;

public class AlloyDBEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(INDENT_OUTPUT);
    private final AlloyDBEngine engine;
    private final String tableName;
    private final String schemaName;
    private final String contentColumn;
    private final String embeddingColumn;
    private final String idColumn;
    private final List<String> metadataColumns;
    private final DistanceStrategy distanceStrategy;
    private final Integer k;
    private final Integer fetchK;
    private final Double lambdaMult;
    // change to QueryOptions class when implemented
    private final List<String> queryOptions;
    private String metadataJsonColumn;

    /**
     * Constructor for AlloyDBEmbeddingStore
     *
     * @param engine The connection object to use
     * @param tableName The name of the table (no default, user must specify)
     * @param schemaName (Optional, Default: "public") The schema name
     * @param contentColumn (Optional, Default: “content”) Column that represent
     * a Document’s page content
     * @param embeddingColumn (Optional, Default: “embedding”) Column for
     * embedding vectors. The embedding is generated from the document value
     * @param idColumn (Optional, Default: "langchain_id") Column to store ids.
     * @param metadataJsonColumn (Default: "langchain_metadata") the column to
     * store extra metadata in
     * @param metadataColumns (Optional) Column(s) that represent a document’s
     * metadata
     * @param ignoreMetadataColumnNames (Optional) Column(s) to ignore in
     * pre-existing tables for a document’s
     * @param distanceStrategy (Defaults: COSINE_DISTANCE) Distance strategy to
     * use for vector similarity search
     * @param k (Defaults: 4) Number of Documents to return from search
     * @param fetchK (Defaults: 20) Number of Documents to fetch to pass to MMR
     * algorithm
     * @param lambdaMult (Defaults: 0.5): Number between 0 and 1 that determines
     * the degree of diversity among the results with 0 corresponding to maximum
     * diversity and 1 to minimum diversity
     * @param queryOptions (Optional) QueryOptions class with vector search
     * parameters
     */
    public AlloyDBEmbeddingStore(Builder builder) {
        this.engine = builder.engine;
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
        this.contentColumn = builder.contentColumn;
        this.embeddingColumn = builder.embeddingColumn;
        this.idColumn = builder.idColumn;
        this.metadataJsonColumn = builder.metadataJsonColumn;
        this.metadataColumns = builder.metadataColumns;
        this.distanceStrategy = builder.distanceStrategy;
        this.k = builder.k;
        this.fetchK = builder.fetchK;
        this.lambdaMult = builder.lambdaMult;
        this.queryOptions = builder.queryOptions;

        // check columns exist in the table
        verifyEmbeddingStoreColumns(builder.ignoreMetadataColumnNames);
    }

    private void verifyEmbeddingStoreColumns(List<String> ignoredColumns) {
        if (!metadataColumns.isEmpty() && !ignoredColumns.isEmpty()) {
            throw new IllegalArgumentException("Cannot use both metadataColumns and ignoreMetadataColumns at the same time.");
        }

        String query = String.format("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \"%s\" AND table_schema = \"%s\"", tableName, schemaName);

        Map<String, String> allColumns = new HashMap();

        try (Connection conn = engine.getConnection(); ResultSet resultSet = conn.createStatement().executeQuery(query)) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                allColumns.put(rsMeta.getColumnName(i), rsMeta.getColumnTypeName(i));
            }

            if (!allColumns.containsKey(idColumn)) {
                throw new IllegalStateException("Id column, " + idColumn + ", does not exist.");
            }
            if (!allColumns.containsKey(contentColumn)) {
                throw new IllegalStateException("Content column, " + contentColumn + ", does not exist.");
            }
            if (!allColumns.get(contentColumn).equalsIgnoreCase("text") || !allColumns.get(contentColumn).contains("char")) {
                throw new IllegalStateException("Content column, is type " + allColumns.get(contentColumn) + ". It must be a type of character string.");
            }
            if (!allColumns.containsKey(embeddingColumn)) {
                throw new IllegalStateException("Embedding column, " + embeddingColumn + ", does not exist.");
            }
            if (!allColumns.get(embeddingColumn).equalsIgnoreCase("USER-DEFINED")) {
                throw new IllegalStateException("Embedding column, " + embeddingColumn + ", is not type Vector.");
            }
            if (!allColumns.containsKey(metadataJsonColumn)) {
                metadataJsonColumn = null;
            }

            for (String metadataColumn : metadataColumns) {
                if (!allColumns.containsKey(metadataColumn)) {
                    throw new IllegalStateException("Metadata column, " + metadataColumn + ", does not exist.");
                }
            }

            if (ignoredColumns != null && !ignoredColumns.isEmpty()) {

                Map<String, String> allColumnsCopy = allColumns.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                ignoredColumns.add(idColumn);
                ignoredColumns.add(contentColumn);
                ignoredColumns.add(embeddingColumn);

                for (String ignore : ignoredColumns) {
                    allColumnsCopy.remove(ignore);
                }

                metadataColumns.addAll(allColumnsCopy.keySet());
            }

        } catch (SQLException ex) {
            throw new RuntimeException("Exception caught when verifying vector store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
        }
    }

    @Override
    public String add(Embedding embedding) {
        String id = randomUUID();
        addInternal(id, embedding, null);
        return id;
    }

    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        String id = randomUUID();
        addInternal(id, embedding, textSegment);
        return id;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        List<TextSegment> nullTextSegments = Collections.nCopies(ids.size(), (TextSegment) null);
        addAll(ids, embeddings, nullTextSegments);
        return ids;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> textSegment) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAll(ids, embeddings, textSegment);
        return ids;
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeAll(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("ids must not be null or empty");
        }

        String query = String.format("DELETE FROM \"%s\".\"%s\" WHERE %s IN (?)", schemaName, tableName, idColumn);

        try (Connection conn = engine.getConnection()) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
                Array array = conn.createArrayOf("uuid", ids.stream().map(UUID::fromString).toArray());
                preparedStatement.setArray(1, array);
                preparedStatement.executeUpdate();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Exception caught when inserting into vector store table: \"%s\".\"%s\"",
                    schemaName, tableName), ex);
        }
    }

    private void addInternal(String id, Embedding embedding, TextSegment textSegment) {
        addAll(singletonList(id), singletonList(embedding), singletonList(textSegment));
    }

    @Override
    public void addAll(List<String> ids, List<Embedding> embeddings, List<TextSegment> textSegments) {
        if (ids.size() != embeddings.size() || embeddings.size() != textSegments.size()) {
            throw new IllegalArgumentException("List parameters ids and embeddings and textSegments shouldn't be different sizes!");
        }
        try (Connection connection = engine.getConnection()) {
            String metadataColumnNames = metadataColumns.stream()
                    .map(column -> "\"" + column + "\"").collect(Collectors.joining(", "));

            // idColumn, contentColumn and embeddedColumn
            int totalColumns = 3;

            if (isNotNullOrEmpty(metadataColumnNames)) {
                totalColumns += metadataColumnNames.split(",").length;
                metadataColumnNames = ", " + metadataColumnNames;
            }

            if (isNotNullOrEmpty(metadataJsonColumn)) {
                metadataColumnNames += ", \"" + metadataJsonColumn + "\"";
                totalColumns++;
            }

            String placeholders = "?";
            for (int p = 1; p < totalColumns; p++) {
                placeholders += ", ?";
            }

            String query = String.format("INSERT INTO \"%s\".\"%s\" (\"%s\", \"%s\", \"%s\"%s) VALUES (%s)", schemaName, tableName, idColumn, contentColumn, embeddingColumn, metadataColumnNames, placeholders);
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                for (int i = 0; i < ids.size(); i++) {
                    String id = ids.get(i);
                    Embedding embedding = embeddings.get(i);
                    TextSegment textSegment = textSegments.get(i);
                    String text = textSegment != null ? textSegment.text() : null;
                    //assume metadata is always present langchain4j/langchain4j-core/src/main/java/dev/langchain4j/data/segment/TextSegment.java L30
                    Map<String, Object> embeddedMetadataCopy = textSegment.metadata().toMap().entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                    preparedStatement.setString(1, id);
                    preparedStatement.setObject(2, embedding);
                    preparedStatement.setString(3, text);
                    int j = 4;
                    if (embeddedMetadataCopy != null && !embeddedMetadataCopy.isEmpty()) {
                        for (; j < metadataColumns.size(); j++) {
                            if (embeddedMetadataCopy.containsKey(metadataColumns.get(j))) {
                                preparedStatement.setObject(j, embeddedMetadataCopy.remove(metadataColumns.get(j)));
                            } else {
                                preparedStatement.setObject(j, null);
                            }
                        }
                        if (isNotNullOrEmpty(metadataJsonColumn)) {
                            // metadataJsonColumn should be the last column left
                            preparedStatement.setObject(j, OBJECT_MAPPER.writeValueAsString(embeddedMetadataCopy), Types.OTHER);
                        }
                    } else {
                        for (; j < metadataColumns.size(); j++) {
                            preparedStatement.setObject(j, null);
                        }
                    }
                    preparedStatement.addBatch();

                }
                preparedStatement.executeBatch();
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Exception caught when processing JSON metadata", ex);
            }

        } catch (SQLException ex) {
            throw new RuntimeException("Exception caught when inserting into vector store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
        }
    }

    public class Builder {

        private AlloyDBEngine engine;
        private String tableName;
        private String schemaName = "public";
        private String contentColumn = "content";
        private String embeddingColumn = "embedding";
        private String idColumn = "langchain_id";
        private List<String> metadataColumns = new ArrayList<>();
        private String metadataJsonColumn = "langchain_metadata";
        private List<String> ignoreMetadataColumnNames = new ArrayList<>();
        private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
        private Integer k = 4;
        private Integer fetchK = 20;
        private Double lambdaMult = 0.5;
        // change to QueryOptions class when implemented
        private List<String> queryOptions;

        public Builder(AlloyDBEngine engine, String tableName) {
            this.engine = engine;
            this.tableName = tableName;
        }

        /**
         * @param schemaName (Default: "public") The schema name
         */
        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        /**
         * @param contentColumn (Default: "content") create the content column
         * with custom name
         */
        public Builder contentColumn(String contentColumn) {
            this.contentColumn = contentColumn;
            return this;
        }

        /**
         * @param embeddingColumn (Default: "embedding") create the embedding
         * column with custom name
         */
        public Builder embeddingColumn(String embeddingColumn) {
            this.embeddingColumn = embeddingColumn;
            return this;
        }

        /**
         * @param idColumn (Optional, Default: "langchain_id") Column to store
         * ids.
         */
        public Builder idColumn(String idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        /**
         * @param metadataColumns list of SQLAlchemy Columns to create for
         * custom metadata
         */
        public Builder metadataColumns(List<String> metadataColumns) {
            this.metadataColumns = metadataColumns;
            return this;
        }

        /**
         * @param metadataJsonColumn (Default: "langchain_metadata") the column
         * to store extra metadata in
         */
        public Builder metadataJsonColumn(String metadataJsonColumn) {
            this.metadataJsonColumn = metadataJsonColumn;
            return this;
        }

        /**
         * @param ignoreMetadataColumnNames (Optional) Column(s) to ignore in
         * pre-existing tables for a document’s
         */
        public Builder ignoreMetadataColumnNames(List<String> ignoreMetadataColumnNames) {
            this.ignoreMetadataColumnNames = ignoreMetadataColumnNames;
            return this;
        }

        /**
         * @param distanceStrategy (Defaults: COSINE_DISTANCE) Distance strategy
         * to use for vector similarity search
         */
        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        /**
         * @param k (Defaults: 4) Number of Documents to return from search
         */
        public Builder k(Integer k) {
            this.k = k;
            return this;
        }

        /**
         * @param fetchK (Defaults: 20) Number of Documents to fetch to pass to
         * MMR algorithm
         */
        public Builder fetchK(Integer fetchK) {
            this.fetchK = fetchK;
            return this;
        }

        /**
         * @param lambdaMult (Defaults: 0.5): Number between 0 and 1 that
         * determines the degree of diversity among the results with 0
         * corresponding to maximum diversity and 1 to minimum diversity
         */
        public Builder lambdaMult(Double lambdaMult) {
            this.lambdaMult = lambdaMult;
            return this;
        }

        /**
         * @param queryOptions (Optional) QueryOptions class with vector search
         * parameters
         */
        public Builder queryOptions(List<String> queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        public AlloyDBEmbeddingStore build() {
            return new AlloyDBEmbeddingStore(this);
        }
    }

}
