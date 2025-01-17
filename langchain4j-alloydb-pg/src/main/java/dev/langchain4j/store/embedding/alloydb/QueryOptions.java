package dev.langchain4j.store.embedding.alloydb;

public class QueryOptions {
    private final Integer efSearch;
    private final Integer probes;

    public QueryOptions(Integer efSearch, Integer probes) {
        this.efSearch = (efSearch != null)?efSearch:40;
        this.probes = (probes != null)?probes:1;
    }

    public Integer getEfSearch() {
        return this.efSearch;
    }

    public Integer getProbes() {
        return this.probes;
    }

}
