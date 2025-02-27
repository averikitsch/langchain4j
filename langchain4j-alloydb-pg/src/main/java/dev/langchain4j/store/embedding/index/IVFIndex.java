package dev.langchain4j.store.embedding.index;

import java.util.List;

public class IVFIndex implements BaseIndex {

    private final String indexType = "ivf";
    private final String name;
    private final Integer listCount;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public IVFIndex(Builder builder) {
        this.name = builder.name;
        this.listCount = builder.listCount;
        this.quantizer = builder.quantizer;
        this.distanceStrategy = builder.distanceStrategy;
        this.partialIndexes = builder.partialIndexes;
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s, quantizer = %s)", listCount, quantizer);
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

    public String getName() {
        return name;
    }
    
    public class Builder {

        private String name;
        private Integer listCount = 100;
        private String quantizer = "sq8";
        private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
        private List<String> partialIndexes;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder listCount(Integer listCount) {
            this.listCount = listCount;
            return this;
        }

        public Builder quantizer(String quantizer) {
            this.quantizer = quantizer;
            return this;
        }

        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public Builder partialIndexes(List<String> partialIndexes) {
            this.partialIndexes = partialIndexes;
            return this;
        }

        public IVFIndex build() {
            return new IVFIndex(this);
        }

    }

}
