package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class ScaNNIndexQueryOptions implements QueryOptions {

    private final Integer numLeavesToSearch;
    private final Integer preOrderingNumNeighbors;

    public ScaNNIndexQueryOptions(Builder builder) {
        this.numLeavesToSearch = builder.numLeavesToSearch;
        this.preOrderingNumNeighbors = builder.preOrderingNumNeighbors;
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList<>();
        parameters.add(String.format("scann.num_leaves_to_search = %s", numLeavesToSearch));
        parameters.add(String.format("scann.pre_reordering_num_neighbors = %s", preOrderingNumNeighbors));
        return parameters;
    }

    public class Builder {

        private Integer numLeavesToSearch = 1;
        private Integer preOrderingNumNeighbors = -1;

        public Builder numLeavesToSearch(Integer numLeavesToSearch) {
            this.numLeavesToSearch = numLeavesToSearch;
            return this;
        }

        public Builder preOrderingNumNeighbors(Integer preOrderingNumNeighbors) {
            this.preOrderingNumNeighbors = preOrderingNumNeighbors;
            return this;
        }

        public ScaNNIndexQueryOptions build() {
            return new ScaNNIndexQueryOptions(this);
        }
    }
}
