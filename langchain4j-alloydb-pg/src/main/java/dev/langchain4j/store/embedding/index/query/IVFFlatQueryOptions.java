package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class IVFFlatQueryOptions implements QueryOptions {

    private final Integer probes;

    public IVFFlatQueryOptions(Builder builder) {
        this.probes = builder.getProbes();
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("ivfflat.probes = %d", probes));
        return parameters;
    }

    public class Builder {

        private Integer probes;

        public Builder() {
            this.probes = 1;
        }

        public Integer getProbes() {
            return probes;
        }

        public Builder probes(Integer probes) {
            this.probes = probes;
            return this;
        }

        public IVFFlatQueryOptions build() {
            return new IVFFlatQueryOptions(this);
        }

    }
}
