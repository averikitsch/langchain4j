package dev.langchain4j.store.embedding.index;

public class IVFFlatIndex implements VectorIndex {

    private final Integer listCount;
    private final DistanceStrategy distanceStrategy;

    public IVFFlatIndex(IndexConfig indexConfig) {
        this.listCount = indexConfig.getListCount();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s)", listCount);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }
}
