package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class HNSWQueryOptions implements QueryOptions {

    private final Integer efSearch;

    public HNSWQueryOptions(Integer efSearch) {
        this.efSearch = efSearch != null ? efSearch : 40;
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("nsw.efS_search = %d", efSearch));
        return parameters;
    }
}
