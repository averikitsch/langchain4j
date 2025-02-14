package dev.langchain4j.store.embedding.index;

import java.util.List;

public class IVFFlatIndex implements VectorIndex {

    private final Integer listCount;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public IVFFlatIndex(IndexConfig indexConfig) {
        this.listCount = indexConfig.getListCount();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
        this.partialIndexes = indexConfig.getPartialIndexes();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s)", listCount);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

    public List<String> getPartialIndexes() {
        return partialIndexes;
    }
}
