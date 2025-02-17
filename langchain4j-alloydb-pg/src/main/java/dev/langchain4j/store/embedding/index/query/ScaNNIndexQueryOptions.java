package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class ScaNNIndexQueryOptions implements QueryOptions {

    private final Integer num_leaves_to_search;
    private final Integer pre_reordering_num_neighbors;

    public ScaNNIndexQueryOptions(Builder builder) {
        this.num_leaves_to_search = builder.getNum_leaves_to_search();
        this.pre_reordering_num_neighbors = builder.getPre_reordering_num_neighbors();
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("scann.num_leaves_to_search = %s", num_leaves_to_search));
        parameters.add(String.format("scann.pre_reordering_num_neighbors = %s", pre_reordering_num_neighbors));
        return parameters;
    }

    public class Builder {

        private Integer num_leaves_to_search;
        private Integer pre_reordering_num_neighbors;

        public Builder() {
            this.num_leaves_to_search = 1;
            this.pre_reordering_num_neighbors = -1;
        }

        public Integer getNum_leaves_to_search() {
            return num_leaves_to_search;
        }

        public Builder num_leaves_to_search(Integer num_leaves_to_search) {
            this.num_leaves_to_search = num_leaves_to_search;
            return this;
        }

        public Integer getPre_reordering_num_neighbors() {
            return pre_reordering_num_neighbors;
        }

        public Builder pre_reordering_num_neighbors(Integer pre_reordering_num_neighbors) {
            this.pre_reordering_num_neighbors = pre_reordering_num_neighbors;
            return this;
        }

        public ScaNNIndexQueryOptions build() {
            return new ScaNNIndexQueryOptions(this);
        }
    }
}
