package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class ScaNNIndexQueryOptions implements QueryOptions {

    private final Integer num_leaves_to_search;
    private final Integer pre_reordering_num_neighbors;

    public ScaNNIndexQueryOptions(Integer num_leaves_to_search, Integer pre_reordering_num_neighbors) {
        this.num_leaves_to_search = (num_leaves_to_search != null) ? num_leaves_to_search : 1;
        this.pre_reordering_num_neighbors = (pre_reordering_num_neighbors != null) ? pre_reordering_num_neighbors : -1;
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("scann.num_leaves_to_search = %s", num_leaves_to_search));
        parameters.add(String.format("scann.pre_reordering_num_neighbors = %s", pre_reordering_num_neighbors));
        return parameters;
    }
}
