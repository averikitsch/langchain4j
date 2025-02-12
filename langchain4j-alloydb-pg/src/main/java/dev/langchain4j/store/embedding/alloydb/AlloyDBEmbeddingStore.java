package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
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
import dev.langchain4j.engine.EmbeddingStoreConfig;
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
    private String schemaName;
    private String contentColumn;
    private String embeddingColumn;
    private String idColumn;
    private String metadataJsonColumn;
    private List<MetadataColumn> metadataColumns;
    private List<String> ignoreMetadataColumnNames;
    private DistanceStrategy distanceStrategy;
    private Integer k;
    private Integer fetchK;
    private Double lambdaMult;
    // change to QueryOptions class when implemented
    private List<String> queryOptions;

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
    public AlloyDBEmbeddingStore(EmbeddingStoreConfig embeddingStoreConfig) {
        this.engine = embeddingStoreConfig.getEngine();
        this.tableName = embeddingStoreConfig.getTableName();
        this.schemaName = embeddingStoreConfig.getSchemaName();
        this.contentColumn = embeddingStoreConfig.getContentColumn();
        this.embeddingColumn = embeddingStoreConfig.getEmbeddingColumn();
        this.idColumn = embeddingStoreConfig.getIdColumn();
        this.metadataJsonColumn = embeddingStoreConfig.getMetadataJsonColumn();
        this.metadataColumns = embeddingStoreConfig.getMetadataColumns();
        this.ignoreMetadataColumnNames = embeddingStoreConfig.getIgnoreMetadataColumnNames();
        this.distanceStrategy = embeddingStoreConfig.getDistanceStrategy();
        this.k = embeddingStoreConfig.getK();
        this.fetchK = embeddingStoreConfig.getFetchK();
        this.lambdaMult = embeddingStoreConfig.getLambdaMult();
        this.queryOptions = embeddingStoreConfig.getQueryOptions();
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
    public String add(Embedding embedding, TextSegment embeddedTextSegment) {
        String id = randomUUID();
        addInternal(id, embedding, embeddedTextSegment);
        return id;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAllInternal(ids, embeddings, null);
        return ids;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embeddedTextSegment) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAllInternal(ids, embeddings, embeddedTextSegment);
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

    private void addInternal(String id, Embedding embedding, TextSegment embeddedTextSegment) {
        addAllInternal(List.of(id), List.of(embedding), List.of(embeddedTextSegment));
    }

    private void addAllInternal(List<String> ids, List<Embedding> embeddings, List<TextSegment> embeddedTextSegments) {
        if (ids.size() != embeddings.size()) {
            throw new IllegalArgumentException("List parameters ids and embeddings shouldn't be different sizes!");
        }
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            Embedding embedding = embeddings.get(i);
            TextSegment embeddedTextSegment = embeddedTextSegments != null ? embeddedTextSegments.get(i) : null;
            try (Connection connection = engine.getConnection()) {
                String embeddedText = embeddedTextSegment != null ? embeddedTextSegment.text() : null;

                String metadataColumnNames = String.format(", %s", metadataColumns.stream()
                        .map(column -> String.format("\"%s\"", column.getName())).collect(Collectors.joining(", ")));
                int totalColumns = metadataColumns.size();

                if (isNotNullOrEmpty(metadataJsonColumn)) {
                    metadataColumnNames = String.format("%s, \"%s\"", metadataColumnNames, metadataJsonColumn);
                    totalColumns++;
                }

                Map<String, Object> embeddedMetadataCopy = null;
                if (embeddedTextSegment != null && embeddedTextSegment.metadata() != null) {
                    embeddedMetadataCopy = embeddedTextSegment.metadata().toMap().entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                }

                // column names separated by comma
                String columnNames = String.format("\"%s\", \"%s\", \"%s\", %s",
                        idColumn, embeddingColumn, contentColumn, metadataColumnNames);
                totalColumns += 3;

                StringBuilder sb = new StringBuilder("?");
                for (int p = 1; p < totalColumns; p++) {
                    sb.append(", ?");
                }

                String placeholders = sb.toString();

                // create query
                String query = String.format("INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)",
                        schemaName, tableName, columnNames, placeholders);
                // prepared statement, add parameters
                try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                    preparedStatement.setString(1, id);
                    preparedStatement.setObject(2, embedding);
                    preparedStatement.setString(3, embeddedText);
                    int j = 4;
                    if (embeddedMetadataCopy != null && !embeddedMetadataCopy.isEmpty()) {
                        for(String ignored: ignoreMetadataColumnNames) {
                            embeddedMetadataCopy.remove(ignored);
                        }
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

                    preparedStatement.executeUpdate();
                } catch (JsonProcessingException ex) {
                    log.error(String.format("Exception caught when inserting into vector store table: \"%s\".\"%s\"",
                            schemaName, tableName), ex);
                    throw new RuntimeException(ex);
                }
            } catch (SQLException ex) {
                log.error(String.format("Exception caught when inserting into vector store table: \"%s\".\"%s\"",
                        schemaName, tableName), ex);
                throw new RuntimeException(ex);
            }
        }

    }

}
