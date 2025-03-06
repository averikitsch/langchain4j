package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class IVFQueryOptions implements QueryOptions {

    private final Integer probes;

    public IVFQueryOptions(Builder builder) {
        this.probes = builder.probes;
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList<>();
        parameters.add(String.format("ivf.probes = %d", probes));
        return parameters;
    }

    public class Builder {

        private Integer probes = 1;

        public Builder probes(Integer probes) {
            this.probes = probes;
            return this;
        }

        public IVFQueryOptions build() {
            return new IVFQueryOptions(this);
        }
    }
}
