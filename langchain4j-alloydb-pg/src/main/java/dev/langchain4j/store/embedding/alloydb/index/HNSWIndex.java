package dev.langchain4j.store.embedding.alloydb.index;

public class HNSWIndex implements VectorIndex {

    private final Integer m;
    private final Integer efConstruction;
    private final Integer efSearch;
    private final DistanceStrategy distanceStrategy;

    public HNSWIndex(Integer m, Integer efConstruction, Integer efSearch, DistanceStrategy distanceStrategy) {
        this.m = (m != null)?m:16;
        this.efConstruction = (efConstruction != null)?efConstruction:64;
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.DistanceStrategyFactory.getCosineDistanceStrategy() ;
        this.efSearch = efSearch;
    }

    @Override
    public String getIndexOptions() {
        return String.format("m = %s, ef_construction = %s", m, efConstruction);
    }

    @Override
    public String getParameterSettings() {
        return String.format("hnsw.efS_search = %d", efSearch);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }
}