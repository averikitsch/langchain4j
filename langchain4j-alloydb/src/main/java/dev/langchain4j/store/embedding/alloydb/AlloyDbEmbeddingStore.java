package dev.langchain4j.store.embedding.alloydb;

import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingStore;

public class AlloyDbEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = Logger.getLogger(AlloyDbEmbeddingStore.class.getName());
    private final DataSource dataSource;

    public AlloyDbEmbeddingStore(String database, String user, String password, String instanceName, String ipType) {
        dataSource = AlloyDbJdbcConnectorDataSourceFactory.createDataSource(database, user, password, instanceName, ipType);
    }

    @Override
    public String add(Embedding embedding) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void add(String id, Embedding embedding) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String add(Embedding embedding, TextSegment embedded) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
