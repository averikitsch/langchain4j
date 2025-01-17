package dev.langchain4j.store.embedding.alloydb.index;

import java.util.Iterator;
import java.util.List;

import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import dev.langchain4j.store.embedding.alloydb.QueryOptions;

public class HNSWIndex implements IndexType {

    private final String tableName;
    private final String embeddingColumn;
    private final Integer m;
    private final Integer efConstruction;
    private final QueryOptions queryOptions;
    private final List<String> partialIndexes;
    private final String distanceStrategy;

    public HNSWIndex(String tableName, String embeddingColumn, Integer m, Integer efConstruction, QueryOptions queryOptions, List<String> partialIndexes, String distanceStrategy) {
        this.tableName = tableName;
        this.embeddingColumn = embeddingColumn;
        this.m = (m != null)?m:16;
        this.efConstruction = (efConstruction != null)?efConstruction:64;
        this.queryOptions = queryOptions;
        this.partialIndexes = partialIndexes;
        this.distanceStrategy = isNotNullOrBlank(distanceStrategy)?distanceStrategy:"vector_l2_ops";
    }
    

    @Override
    public String generateCreateIndexQuery() {
        String query = String.format("CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (%s %s) WITH (m = %d, ef_construction = %d)",
        tableName + "_hnsw_index", tableName, embeddingColumn, distanceStrategy, m, efConstruction);

        if(partialIndexes != null){
            StringBuilder sb = new StringBuilder(query);
            sb.append(" WHERE ");
            Iterator<String> it = partialIndexes.iterator();
            while(it.hasNext()) {
                String partialIndex = it.next();
                sb.append(partialIndex);
                if(it.hasNext()) sb.append(" AND ");
            }
            return sb.toString();
        }

        return query;
    }

    @Override
    public String generateParameterSetting() {
        if(queryOptions == null || queryOptions.getEfSearch() == null){
            return "";
        }
        return String.format("SET hnsw.efS_search = %d", queryOptions.getEfSearch());
    }
}
