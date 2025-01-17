package dev.langchain4j.store.embedding.alloydb.index;

import java.util.Iterator;
import java.util.List;

import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import dev.langchain4j.store.embedding.alloydb.QueryOptions;

public class IVFFlatIndex implements VectorIndex {

    private final String tableName;
    private final String embeddingColumn;
    private final Integer listCount;
    private final QueryOptions queryOptions;
    private final List<String> partialIndexes;
    private final String distanceStrategy;

    public IVFFlatIndex(String tableName, String embeddingColumn, Integer listCount, QueryOptions queryOptions, List<String> partialIndexes, String distanceStrategy) {
        this.tableName = tableName;
        this.embeddingColumn = embeddingColumn;
        this.listCount = (listCount != null) ? listCount : 1;
        this.queryOptions = queryOptions;
        this.partialIndexes = partialIndexes;
        this.distanceStrategy = isNotNullOrBlank(distanceStrategy) ? distanceStrategy : "vector_l2_ops";
    }

    @Override
    public String generateCreateIndexQuery() {
        String query = String.format("CREATE INDEX IF NOT EXISTS %s ON %s USING ivfflat (%s %s) WITH (lists = %s)",
                tableName + "_ivfflat_index", tableName, embeddingColumn, distanceStrategy, listCount);

        if (partialIndexes != null) {
            StringBuilder sb = new StringBuilder(query);
            sb.append(" WHERE ");
            Iterator<String> it = partialIndexes.iterator();
            while (it.hasNext()) {
                String partialIndex = it.next();
                sb.append(partialIndex);
                if (it.hasNext()) {
                    sb.append(" AND ");
                }
            }
            return sb.toString();
        }

        return query;
    }

    @Override
    public String generateParameterSetting() {
        if (queryOptions == null || queryOptions.getEfSearch() == null) {
            return "";
        }
        return String.format("SET ivfflat.probes = %d", queryOptions.getProbes());
    }

}
