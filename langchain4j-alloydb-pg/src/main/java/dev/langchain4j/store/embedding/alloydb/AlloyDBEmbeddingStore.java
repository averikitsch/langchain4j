package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.AlloyDBEngine;
import dev.langchain4j.engine.MetadataColumn;
import static dev.langchain4j.internal.Utils.isNotNullOrEmpty;
import static dev.langchain4j.internal.Utils.randomUUID;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;

public class AlloyDBEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(INDENT_OUTPUT);
    private static final Logger log = LoggerFactory.getLogger(AlloyDBEmbeddingStore.class.getName());
    private final AlloyDBEngine engine;
    private final String tableName;
    private final String schemaName;
    private final String contentColumn;
    private final String embeddingColumn;
    private final String idColumn;
    private final List<MetadataColumn> metadataColumns;
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
    public AlloyDBEmbeddingStore(String tableName, String schemaName, String contentColumn, String embeddingColumn,
            String idColumn, List<MetadataColumn> metadataColumns, String metadataJsonColumn, List<String> ignoreMetadataColumnNames,
            DistanceStrategy distanceStrategy, Integer k, Integer fetchK, Double lambdaMult, QueryOptions queryOptions, AlloyDBEngine engine) {
        this.engine = engine;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.contentColumn = contentColumn;
        this.embeddingColumn = embeddingColumn;
        this.idColumn = idColumn;
        this.metadataJsonColumn = metadataJsonColumn;
        this.metadataColumns = metadataColumns;
        this.distanceStrategy = distanceStrategy;
        this.k = k;
        this.fetchK = fetchK;
        this.lambdaMult = lambdaMult;
        this.queryOptions = queryOptions;

        // check columns exist in the table
        verifyEmbeddingStoreColumns(ignoreMetadataColumnNames);
    }

    private void verifyEmbeddingStoreColumns(List<String> ignoredColumns) {
        if (!metadataColumns.isEmpty()) {
            if (!ignoredColumns.isEmpty()) {
                throw new IllegalArgumentException("Cannot use both metadataColumns and ignoreMetadataColumns at the same time.");
            }
        }

        String query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \"" + tableName + "\" AND table_schema = \"" + schemaName + "\"";

        Map<String, String> allColumns = new HashMap();

        try (ResultSet resultSet = engine.getConnection().createStatement().executeQuery(query)) {
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

            for (MetadataColumn metadataColumn : metadataColumns) {
                if (!allColumns.containsKey(metadataColumn.getName())) {
                    throw new IllegalStateException("Metadata column, " + metadataColumn.getName() + ", is not type Vector.");
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

                metadataColumns.addAll((allColumnsCopy.entrySet().stream()
                        .map(e -> new MetadataColumn(e.getKey(), e.getValue(), true)).collect(Collectors.toList())));
            }

        } catch (SQLException ex) {
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
        List<TextSegment> nullTextSegments =  Collections.nCopies(ids.size(), (TextSegment) null);
        addAllInternal(ids, embeddings, nullTextSegments);
        return ids;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> textSegment) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAllInternal(ids, embeddings, textSegment);
        return ids;
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeAll(Collection<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void addInternal(String id, Embedding embedding, TextSegment textSegment) {
        addAllInternal(List.of(id), List.of(embedding), List.of(textSegment));
    }

    private void addAllInternal(List<String> ids, List<Embedding> embeddings, List<TextSegment> textSegments) {
        if (ids.size() != embeddings.size() || embeddings.size() != textSegments.size()) {
            throw new IllegalArgumentException("List parameters ids and embeddings and textSegments shouldn't be different sizes!");
        }
        try (Connection connection = engine.getConnection()) {
            String metadataColumnNames = ", " + metadataColumns.stream()
                    .map(column -> "\"" + column.getName() + "\"").collect(Collectors.joining(", "));
            int totalColumns = metadataColumns.size();

            if (isNotNullOrEmpty(metadataJsonColumn)) {
                metadataColumnNames += ", \"" + metadataJsonColumn + "\"";
                totalColumns++;
            }
            // column names separated by comma
            String columnNames = "\"" + idColumn + "\", \"" + embeddingColumn + "\", \"" + contentColumn + "\"," + metadataColumnNames;
            totalColumns += 3;

            String placeholders = "?";
            for (int p = 1; p < totalColumns; p++) {
                placeholders += ", ?";
            }

            String query = "INSERT INTO \"" + schemaName + "\".\"" + tableName + "\" (" + columnNames + ") VALUES (" + placeholders + ");";

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
                            if (embeddedMetadataCopy.containsKey(metadataColumns.get(j).getName())) {
                                preparedStatement.setObject(j, embeddedMetadataCopy.remove(metadataColumns.get(j).getName()));
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
                preparedStatement.executeUpdate();

            } catch (JsonProcessingException ex) {
                log.error("Exception caught when inserting into vector store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
                throw new RuntimeException(ex);
            }

        } catch (SQLException ex) {
            log.error("Exception caught when inserting into vector store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
            throw new RuntimeException(ex);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String tableName;
        private String schemaName;
        private String contentColumn;
        private String embeddingColumn;
        private String idColumn;
        private List<MetadataColumn> metadataColumns;
        private String metadataJsonColumn;
        private List<String> ignoreMetadataColumnNames;
        private DistanceStrategy distanceStrategy;
        private Integer k;
        private Integer fetchK;
        private Double lambdaMult;
        // change to QueryOptions class when implemented
        private List<String> queryOptions;
        private AlloyDBEngine engine;

        public Builder() {
            this.schemaName = "public";
            this.contentColumn = "content";
            this.embeddingColumn = "embedding";
            this.idColumn = "langchain_id";
            this.metadataJsonColumn = "langchain_metadata";
            this.metadataColumns = new ArrayList<>();
            this.ignoreMetadataColumnNames = new ArrayList();
            this.distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
            this.k = 4;
            this.fetchK = 20;
            this.lambdaMult = 0.5;
        }

        /**
         * @param tableName (Required) the table name to create - does not
         * append a suffix or prefix!
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
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
        public Builder metadataColumns(List<MetadataColumn> metadataColumns) {
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

        /**
         * @param engine The connection object to use
         *
         */
        public Builder engine(AlloyDBEngine engine) {
            this.engine = engine;
            return this;
        }

        public AlloyDBEmbeddingStore build() {
            return new AlloyDBEmbeddingStore(tableName, schemaName, contentColumn, embeddingColumn,
                    idColumn, metadataColumns, metadataJsonColumn, ignoreMetadataColumnNames,
                    distanceStrategy, k, fetchK, lambdaMult, queryOptions, engine);
        }
    }

}
