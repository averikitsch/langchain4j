package dev.langchain4j.store.embedding.index;

import java.util.ArrayList;
import java.util.List;

public class IVFFlatIndex implements BaseIndex {

    private final String indexType;
    private final Integer listCount;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public IVFFlatIndex(Builder builder) {
        this.indexType = builder.getIndexType();
        this.listCount = builder.getListCount();
        this.distanceStrategy = builder.getDistanceStrategy();
        this.partialIndexes = builder.getPartialIndexes();
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

    public String getIndexType() {
        return indexType;
    }

    public class Builder {

        private String indexType;
        private Integer listCount;
        private DistanceStrategy distanceStrategy;
        private List<String> partialIndexes;

        public Builder() {
            this.indexType = DEFAULT_INDEX_NAME_SUFFIX;
            this.listCount = 100;
            this.distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
            this.partialIndexes = new ArrayList<>();
        }

        public String getIndexType() {
            return indexType;
        }

        public Builder indexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        public Integer getListCount() {
            return listCount;
        }

        public Builder listCount(Integer listCount) {
            this.listCount = listCount;
            return this;
        }

        public DistanceStrategy getDistanceStrategy() {
            return distanceStrategy;
        }

        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public List<String> getPartialIndexes() {
            return partialIndexes;
        }

        public Builder partialIndexes(List<String> partialIndexes) {
            this.partialIndexes = partialIndexes;
            return this;
        }

        public IVFFlatIndex build() {
            return new IVFFlatIndex(this);
        }
    }
}
