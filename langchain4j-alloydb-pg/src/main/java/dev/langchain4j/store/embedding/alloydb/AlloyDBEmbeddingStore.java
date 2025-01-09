package dev.langchain4j.store.embedding.alloydb;

import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;

public class AlloyDBEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = Logger.getLogger(AlloyDBEmbeddingStore.class.getName());
    private final AlloyDBEngine engine;

    public AlloyDBEmbeddingStore(String database,
            String user,
            String password,
            String project_id,
            String cluster,
            String region,
            String instance,
            String ipType,
            String iamAccountEmail
    ) {
        engine = new AlloyDBEngine(database, user, password, project_id, cluster, region, instance, ipType, iamAccountEmail);
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

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeAll(Collection<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
