package dev.langchain4j.store.embedding.alloydb.index;

import java.util.List;

public class HSNWIndex implements IndexType {

    private String tableName;
    private String embeddingColumn;
    private Integer m;
    private Integer efConstruction;
    private QueryOptions queryOptions;
    private List<String> partialIndexes;
    private String distanceStrategy;

    public HSNWIndex() {
        // TODO Auto-generated method stub
    }

    @Override
    public String generateCreateIndexQuery() {
        return String.format("CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (%s vector_cosine_ops) WITH (m = %d, ef_construction = %d)", tableName + "_hnsw_index", tableName, embeddingColumn, m, efConstruction);
    }
}
