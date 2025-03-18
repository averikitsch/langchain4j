package dev.langchain4j.store.embedding.alloydb;

import static org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils.nextInt;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.AlloyDBEngine;
import dev.langchain4j.engine.EmbeddingStoreConfig;
import dev.langchain4j.engine.MetadataColumn;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreWithFilteringIT;
import dev.langchain4j.store.embedding.index.DistanceStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class AlloyDBEmbeddingStoreIT extends EmbeddingStoreWithFilteringIT {

    @Container
    static PostgreSQLContainer<?> pgVector =
            new PostgreSQLContainer<>("pgvector/pgvector:pg15").withCommand("postgres -c max_connections=100");

    final String tableName = "test" + nextInt(2000, 3000);
    static AlloyDBEngine engine;
    EmbeddingStore<TextSegment> embeddingStore;
    EmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();

    @BeforeAll
    public static void startEngine() {
        if (engine == null) {
            engine = new AlloyDBEngine.Builder()
                    .host(pgVector.getHost())
                    .port(pgVector.getFirstMappedPort())
                    .user("test")
                    .password("test")
                    .database("test")
                    .build();
        }
    }

    @AfterAll
    public static void stopEngine() {
        engine.close();
    }

    @Override
    protected void ensureStoreIsReady() {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("name", "text", true));
        metadataColumns.add(new MetadataColumn("name2", "text", true));
        metadataColumns.add(new MetadataColumn("city", "text", true));
        metadataColumns.add(new MetadataColumn("age", "integer", true));

        engine.initVectorStoreTable(new EmbeddingStoreConfig.Builder(tableName, 384)
                .metadataColumns(metadataColumns)
                .overwriteExisting(true)
                .build());

        List<String> metadataColumnNames =
                metadataColumns.stream().map(c -> c.getName()).collect(Collectors.toList());

        embeddingStore = new AlloyDBEmbeddingStore.Builder(engine, tableName)
                .distanceStrategy(DistanceStrategy.COSINE_DISTANCE)
                .metadataColumns(metadataColumnNames)
                .build();
    }

    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        if (embeddingStore == null) {
            ensureStoreIsReady();
        }
        return embeddingStore;
    }

    @Override
    protected EmbeddingModel embeddingModel() {
        return embeddingModel;
    }

    @Override
    protected boolean supportsContains() {
        return true;
    }

    @Override
    protected static Stream<Arguments> should_filter_by_metadata() {
        return Stream.<Arguments>builder()

                // === Equal ===

                .add(Arguments.of(
                        metadataKey("key").isEqualTo("a"),
                        asList(
                                new Metadata().put("key", "a"),
                                new Metadata().put("key", "a").put("key2", "b")),
                        asList(
                                new Metadata().put("key", "A"),
                                new Metadata().put("key", "b"),
                                new Metadata().put("key", "aa"),
                                new Metadata().put("key", "a a"),
                                new Metadata().put("key2", "a"),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isEqualTo(TEST_UUID),
                        asList(
                                new Metadata().put("key", TEST_UUID),
                                new Metadata().put("key", TEST_UUID).put("key2", UUID.randomUUID())),
                        asList(
                                new Metadata().put("key", UUID.randomUUID()),
                                new Metadata().put("key2", TEST_UUID),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isEqualTo(1),
                        asList(
                                new Metadata().put("key", 1),
                                new Metadata().put("key", 1).put("key2", 0)),
                        asList(
                                new Metadata().put("key", -1),
                                new Metadata().put("key", 0),
                                new Metadata().put("key2", 1),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isEqualTo(1L),
                        asList(
                                new Metadata().put("key", 1L),
                                new Metadata().put("key", 1L).put("key2", 0L)),
                        asList(
                                new Metadata().put("key", -1L),
                                new Metadata().put("key", 0L),
                                new Metadata().put("key2", 1L),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isEqualTo(1.23f),
                        asList(
                                new Metadata().put("key", 1.23f),
                                new Metadata().put("key", 1.23f).put("key2", 0f)),
                        asList(
                                new Metadata().put("key", -1.23f),
                                new Metadata().put("key", 1.22f),
                                new Metadata().put("key", 1.24f),
                                new Metadata().put("key2", 1.23f),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isEqualTo(1.23d),
                        asList(
                                new Metadata().put("key", 1.23d),
                                new Metadata().put("key", 1.23d).put("key2", 0d)),
                        asList(
                                new Metadata().put("key", -1.23d),
                                new Metadata().put("key", 1.22d),
                                new Metadata().put("key", 1.24d),
                                new Metadata().put("key2", 1.23d),
                                new Metadata())))

                // === GreaterThan ==

                .add(Arguments.of(
                        metadataKey("key").isGreaterThan("b"),
                        asList(
                                new Metadata().put("key", "c"),
                                new Metadata().put("key", "c").put("key2", "a")),
                        asList(
                                new Metadata().put("key", "a"),
                                new Metadata().put("key", "b"),
                                new Metadata().put("key2", "c"),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThan(1),
                        asList(
                                new Metadata().put("key", 2),
                                new Metadata().put("key", 2).put("key2", 0)),
                        asList(
                                new Metadata().put("key", -2),
                                new Metadata().put("key", 0),
                                new Metadata().put("key", 1),
                                new Metadata().put("key2", 2),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThan(1L),
                        asList(
                                new Metadata().put("key", 2L),
                                new Metadata().put("key", 2L).put("key2", 0L)),
                        asList(
                                new Metadata().put("key", -2L),
                                new Metadata().put("key", 0L),
                                new Metadata().put("key", 1L),
                                new Metadata().put("key2", 2L),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThan(1.1f),
                        asList(
                                new Metadata().put("key", 1.2f),
                                new Metadata().put("key", 1.2f).put("key2", 1.0f)),
                        asList(
                                new Metadata().put("key", -1.2f),
                                new Metadata().put("key", 0.0f),
                                new Metadata().put("key", 1.1f),
                                new Metadata().put("key2", 1.2f),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThan(1.1d),
                        asList(
                                new Metadata().put("key", 1.2d),
                                new Metadata().put("key", 1.2d).put("key2", 1.0d)),
                        asList(
                                new Metadata().put("key", -1.2d),
                                new Metadata().put("key", 0.0d),
                                new Metadata().put("key", 1.1d),
                                new Metadata().put("key2", 1.2d),
                                new Metadata())))

                // === GreaterThanOrEqual ==

                .add(Arguments.of(
                        metadataKey("key").isGreaterThanOrEqualTo("b"),
                        asList(
                                new Metadata().put("key", "b"),
                                new Metadata().put("key", "c"),
                                new Metadata().put("key", "c").put("key2", "a")),
                        asList(new Metadata().put("key", "a"), new Metadata().put("key2", "b"), new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThanOrEqualTo(1),
                        asList(
                                new Metadata().put("key", 1),
                                new Metadata().put("key", 2),
                                new Metadata().put("key", 2).put("key2", 0)),
                        asList(
                                new Metadata().put("key", -2),
                                new Metadata().put("key", -1),
                                new Metadata().put("key", 0),
                                new Metadata().put("key2", 1),
                                new Metadata().put("key2", 2),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThanOrEqualTo(1L),
                        asList(
                                new Metadata().put("key", 1L),
                                new Metadata().put("key", 2L),
                                new Metadata().put("key", 2L).put("key2", 0L)),
                        asList(
                                new Metadata().put("key", -2L),
                                new Metadata().put("key", -1L),
                                new Metadata().put("key", 0L),
                                new Metadata().put("key2", 1L),
                                new Metadata().put("key2", 2L),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThanOrEqualTo(1.1f),
                        asList(
                                new Metadata().put("key", 1.1f),
                                new Metadata().put("key", 1.2f),
                                new Metadata().put("key", 1.2f).put("key2", 1.0f)),
                        asList(
                                new Metadata().put("key", -1.2f),
                                new Metadata().put("key", -1.1f),
                                new Metadata().put("key", 0.0f),
                                new Metadata().put("key2", 1.1f),
                                new Metadata().put("key2", 1.2f),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isGreaterThanOrEqualTo(1.1d),
                        asList(
                                new Metadata().put("key", 1.1d),
                                new Metadata().put("key", 1.2d),
                                new Metadata().put("key", 1.2d).put("key2", 1.0d)),
                        asList(
                                new Metadata().put("key", -1.2d),
                                new Metadata().put("key", -1.1d),
                                new Metadata().put("key", 0.0d),
                                new Metadata().put("key2", 1.1d),
                                new Metadata().put("key2", 1.2d),
                                new Metadata())))

                // === LessThan ==

                .add(Arguments.of(
                        metadataKey("key").isLessThan("b"),
                        asList(
                                new Metadata().put("key", "a"),
                                new Metadata().put("key", "a").put("key2", "c")),
                        asList(
                                new Metadata().put("key", "b"),
                                new Metadata().put("key", "c"),
                                new Metadata().put("key2", "a"),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThan(1),
                        asList(
                                new Metadata().put("key", -2),
                                new Metadata().put("key", 0),
                                new Metadata().put("key", 0).put("key2", 2)),
                        asList(
                                new Metadata().put("key", 1),
                                new Metadata().put("key", 2),
                                new Metadata().put("key2", 0),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThan(1L),
                        asList(
                                new Metadata().put("key", -2L),
                                new Metadata().put("key", 0L),
                                new Metadata().put("key", 0L).put("key2", 2L)),
                        asList(
                                new Metadata().put("key", 1L),
                                new Metadata().put("key", 2L),
                                new Metadata().put("key2", 0L),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThan(1.1f),
                        asList(
                                new Metadata().put("key", -1.2f),
                                new Metadata().put("key", 1.0f),
                                new Metadata().put("key", 1.0f).put("key2", 1.2f)),
                        asList(
                                new Metadata().put("key", 1.1f),
                                new Metadata().put("key", 1.2f),
                                new Metadata().put("key2", 1.0f),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThan(1.1d),
                        asList(
                                new Metadata().put("key", -1.2d),
                                new Metadata().put("key", 1.0d),
                                new Metadata().put("key", 1.0d).put("key2", 1.2d)),
                        asList(
                                new Metadata().put("key", 1.1d),
                                new Metadata().put("key", 1.2d),
                                new Metadata().put("key2", 1.0d),
                                new Metadata())))

                // === LessThanOrEqual ==

                .add(Arguments.of(
                        metadataKey("key").isLessThanOrEqualTo("b"),
                        asList(
                                new Metadata().put("key", "a"),
                                new Metadata().put("key", "b"),
                                new Metadata().put("key", "b").put("key2", "c")),
                        asList(new Metadata().put("key", "c"), new Metadata().put("key2", "a"), new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThanOrEqualTo(1),
                        asList(
                                new Metadata().put("key", -2),
                                new Metadata().put("key", 0),
                                new Metadata().put("key", 1),
                                new Metadata().put("key", 1).put("key2", 2)),
                        asList(new Metadata().put("key", 2), new Metadata().put("key2", 0), new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThanOrEqualTo(1L),
                        asList(
                                new Metadata().put("key", -2L),
                                new Metadata().put("key", 0L),
                                new Metadata().put("key", 1L),
                                new Metadata().put("key", 1L).put("key2", 2L)),
                        asList(new Metadata().put("key", 2L), new Metadata().put("key2", 0L), new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThanOrEqualTo(1.1f),
                        asList(
                                new Metadata().put("key", -1.2f),
                                new Metadata().put("key", 1.0f),
                                new Metadata().put("key", 1.1f),
                                new Metadata().put("key", 1.1f).put("key2", 1.2f)),
                        asList(new Metadata().put("key", 1.2f), new Metadata().put("key2", 1.0f), new Metadata())))
                .add(Arguments.of(
                        metadataKey("key").isLessThanOrEqualTo(1.1d),
                        asList(
                                new Metadata().put("key", -1.2d),
                                new Metadata().put("key", 1.0d),
                                new Metadata().put("key", 1.1d),
                                new Metadata().put("key", 1.1d).put("key2", 1.2d)),
                        asList(new Metadata().put("key", 1.2d), new Metadata().put("key2", 1.0d), new Metadata())))

                // === In ===

                // In: string
                .add(Arguments.of(
                        metadataKey("name").isIn("Klaus"),
                        asList(
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("age", 42)),
                        asList(
                                new Metadata().put("name", "Klaus Heisler"),
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("name2", "Klaus"),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("name").isIn(singletonList("Klaus")),
                        asList(
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("age", 42)),
                        asList(
                                new Metadata().put("name", "Klaus Heisler"),
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("name2", "Klaus"),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("name").isIn("Klaus", "Alice"),
                        asList(
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("name", "Alice").put("age", 42)),
                        asList(
                                new Metadata().put("name", "Klaus Heisler"),
                                new Metadata().put("name", "Zoe"),
                                new Metadata().put("name2", "Klaus"),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("name").isIn(asList("Klaus", "Alice")),
                        asList(
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("name", "Alice").put("age", 42)),
                        asList(
                                new Metadata().put("name", "Klaus Heisler"),
                                new Metadata().put("name", "Zoe"),
                                new Metadata().put("name2", "Klaus"),
                                new Metadata())))

                // In: UUID
                .add(Arguments.of(
                        metadataKey("name").isIn(TEST_UUID),
                        asList(
                                new Metadata().put("name", TEST_UUID),
                                new Metadata().put("name", TEST_UUID).put("age", 42)),
                        asList(
                                new Metadata().put("name", UUID.randomUUID()),
                                new Metadata().put("name2", TEST_UUID),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("name").isIn(singletonList(TEST_UUID)),
                        asList(
                                new Metadata().put("name", TEST_UUID),
                                new Metadata().put("name", TEST_UUID).put("age", 42)),
                        asList(
                                new Metadata().put("name", UUID.randomUUID()),
                                new Metadata().put("name2", TEST_UUID),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("name").isIn(TEST_UUID, TEST_UUID2),
                        asList(
                                new Metadata().put("name", TEST_UUID),
                                new Metadata().put("name", TEST_UUID).put("age", 42),
                                new Metadata().put("name", TEST_UUID2),
                                new Metadata().put("name", TEST_UUID2).put("age", 42)),
                        asList(
                                new Metadata().put("name", UUID.randomUUID()),
                                new Metadata().put("name2", TEST_UUID),
                                new Metadata())))
                .add(Arguments.of(
                        metadataKey("name").isIn(asList(TEST_UUID, TEST_UUID2)),
                        asList(
                                new Metadata().put("name", TEST_UUID),
                                new Metadata().put("name", TEST_UUID).put("age", 42),
                                new Metadata().put("name", TEST_UUID2),
                                new Metadata().put("name", TEST_UUID2).put("age", 42)),
                        asList(
                                new Metadata().put("name", UUID.randomUUID()),
                                new Metadata().put("name2", TEST_UUID),
                                new Metadata())))

                // In: integer
                .add(Arguments.of(
                        metadataKey("age").isIn(42),
                        asList(
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 42).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666), new Metadata().put("age2", 42), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(singletonList(42)),
                        asList(
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 42).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666), new Metadata().put("age2", 42), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(42, 18),
                        asList(
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 18),
                                new Metadata().put("age", 42).put("name", "Klaus"),
                                new Metadata().put("age", 18).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666), new Metadata().put("age2", 42), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(asList(42, 18)),
                        asList(
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 18),
                                new Metadata().put("age", 42).put("name", "Klaus"),
                                new Metadata().put("age", 18).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666), new Metadata().put("age2", 42), new Metadata())))

                // In: long
                .add(Arguments.of(
                        metadataKey("age").isIn(42L),
                        asList(
                                new Metadata().put("age", 42L),
                                new Metadata().put("age", 42L).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666L), new Metadata().put("age2", 42L), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(singletonList(42L)),
                        asList(
                                new Metadata().put("age", 42L),
                                new Metadata().put("age", 42L).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666L), new Metadata().put("age2", 42L), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(42L, 18L),
                        asList(
                                new Metadata().put("age", 42L),
                                new Metadata().put("age", 18L),
                                new Metadata().put("age", 42L).put("name", "Klaus"),
                                new Metadata().put("age", 18L).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666L), new Metadata().put("age2", 42L), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(asList(42L, 18L)),
                        asList(
                                new Metadata().put("age", 42L),
                                new Metadata().put("age", 18L),
                                new Metadata().put("age", 42L).put("name", "Klaus"),
                                new Metadata().put("age", 18L).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666L), new Metadata().put("age2", 42L), new Metadata())))

                // In: float
                .add(Arguments.of(
                        metadataKey("age").isIn(42.0f),
                        asList(
                                new Metadata().put("age", 42.0f),
                                new Metadata().put("age", 42.0f).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666.0f), new Metadata().put("age2", 42.0f), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(singletonList(42.0f)),
                        asList(
                                new Metadata().put("age", 42.0f),
                                new Metadata().put("age", 42.0f).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666.0f), new Metadata().put("age2", 42.0f), new Metadata())))

                // In: double
                .add(Arguments.of(
                        metadataKey("age").isIn(42.0d),
                        asList(
                                new Metadata().put("age", 42.0d),
                                new Metadata().put("age", 42.0d).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666.0d), new Metadata().put("age2", 42.0d), new Metadata())))
                .add(Arguments.of(
                        metadataKey("age").isIn(singletonList(42.0d)),
                        asList(
                                new Metadata().put("age", 42.0d),
                                new Metadata().put("age", 42.0d).put("name", "Klaus")),
                        asList(new Metadata().put("age", 666.0d), new Metadata().put("age2", 42.0d), new Metadata())))

                // === Or ===

                // Or: one key
                .add(Arguments.of(
                        or(
                                metadataKey("name").isEqualTo("Klaus"),
                                metadataKey("name").isEqualTo("Alice")),
                        asList(
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("name", "Alice").put("age", 42)),
                        asList(new Metadata().put("name", "Zoe"), new Metadata())))
                .add(Arguments.of(
                        or(
                                metadataKey("name").isEqualTo("Alice"),
                                metadataKey("name").isEqualTo("Klaus")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("name", "Alice").put("age", 42),
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("age", 42)),
                        asList(new Metadata().put("name", "Zoe"), new Metadata())))

                // Or: multiple keys
                .add(Arguments.of(
                        or(
                                metadataKey("name").isEqualTo("Klaus"),
                                metadataKey("age").isEqualTo(42)),
                        asList(
                                // only Or.left is present and true
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("city", "Munich"),

                                // Or.left is true, Or.right is false
                                new Metadata().put("name", "Klaus").put("age", 666),

                                // only Or.right is present and true
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 42).put("city", "Munich"),

                                // Or.right is true, Or.left is false
                                new Metadata().put("age", 42).put("name", "Alice"),

                                // Or.left and Or.right are both true
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("age", 666),
                                new Metadata().put("name", "Alice").put("age", 666),
                                new Metadata())))
                .add(Arguments.of(
                        or(metadataKey("age").isEqualTo(42), metadataKey("name").isEqualTo("Klaus")),
                        asList(
                                // only Or.left is present and true
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 42).put("city", "Munich"),

                                // Or.left is true, Or.right is false
                                new Metadata().put("age", 42).put("name", "Alice"),

                                // only Or.right is present and true
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("city", "Munich"),

                                // Or.right is true, Or.left is false
                                new Metadata().put("name", "Klaus").put("age", 666),

                                // Or.left and Or.right are both true
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("age", 666),
                                new Metadata().put("name", "Alice").put("age", 666),
                                new Metadata())))

                // Or: x2
                .add(Arguments.of(
                        or(
                                metadataKey("name").isEqualTo("Klaus"),
                                or(
                                        metadataKey("age").isEqualTo(42),
                                        metadataKey("city").isEqualTo("Munich"))),
                        asList(
                                // only Or.left is present and true
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("country", "Germany"),

                                // Or.left is true, Or.right is false
                                new Metadata().put("name", "Klaus").put("age", 666),
                                new Metadata().put("name", "Klaus").put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 666)
                                        .put("city", "Frankfurt"),

                                // only Or.right is present and true
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 42).put("country", "Germany"),
                                new Metadata().put("city", "Munich"),
                                new Metadata().put("city", "Munich").put("country", "Germany"),
                                new Metadata().put("age", 42).put("city", "Munich"),
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany"),

                                // Or.right is true, Or.left is false
                                new Metadata().put("age", 42).put("name", "Alice"),
                                new Metadata().put("city", "Munich").put("name", "Alice"),
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("name", "Alice"),

                                // Or.left and Or.right are both true
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("country", "Germany"),
                                new Metadata().put("name", "Klaus").put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("city", "Munich")
                                        .put("country", "Germany"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("age", 666),
                                new Metadata().put("city", "Frankfurt"),
                                new Metadata().put("name", "Alice").put("age", 666),
                                new Metadata().put("name", "Alice").put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Alice")
                                        .put("age", 666)
                                        .put("city", "Frankfurt"),
                                new Metadata())))
                .add(Arguments.of(
                        or(
                                or(
                                        metadataKey("name").isEqualTo("Klaus"),
                                        metadataKey("age").isEqualTo(42)),
                                metadataKey("city").isEqualTo("Munich")),
                        asList(
                                // only Or.left is present and true
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("country", "Germany"),
                                new Metadata().put("age", 42),
                                new Metadata().put("age", 42).put("country", "Germany"),
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("country", "Germany"),

                                // Or.left is true, Or.right is false
                                new Metadata().put("name", "Klaus").put("city", "Frankfurt"),
                                new Metadata().put("age", 42).put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Frankfurt"),

                                // only Or.right is present and true
                                new Metadata().put("city", "Munich"),
                                new Metadata().put("city", "Munich").put("country", "Germany"),

                                // Or.right is true, Or.left is false
                                new Metadata().put("city", "Munich").put("name", "Alice"),
                                new Metadata().put("city", "Munich").put("age", 666),

                                // Or.left and Or.right are both true
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("country", "Germany"),
                                new Metadata().put("name", "Klaus").put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("city", "Munich")
                                        .put("country", "Germany"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("age", 666),
                                new Metadata().put("city", "Frankfurt"),
                                new Metadata().put("name", "Alice").put("age", 666),
                                new Metadata().put("name", "Alice").put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Alice")
                                        .put("age", 666)
                                        .put("city", "Frankfurt"),
                                new Metadata())))

                // === AND ===

                .add(Arguments.of(
                        and(
                                metadataKey("name").isEqualTo("Klaus"),
                                metadataKey("age").isEqualTo(42)),
                        asList(
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")),
                        asList(
                                // only And.left is present and true
                                new Metadata().put("name", "Klaus"),

                                // And.left is true, And.right is false
                                new Metadata().put("name", "Klaus").put("age", 666),

                                // only And.right is present and true
                                new Metadata().put("age", 42),

                                // And.right is true, And.left is false
                                new Metadata().put("age", 42).put("name", "Alice"),

                                // And.left, And.right are both false
                                new Metadata().put("age", 666).put("name", "Alice"),
                                new Metadata())))
                .add(Arguments.of(
                        and(
                                metadataKey("age").isEqualTo(42),
                                metadataKey("name").isEqualTo("Klaus")),
                        asList(
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")),
                        asList(
                                // only And.left is present and true
                                new Metadata().put("age", 42),

                                // And.left is true, And.right is false
                                new Metadata().put("age", 42).put("name", "Alice"),

                                // only And.right is present and true
                                new Metadata().put("name", "Klaus"),

                                // And.right is true, And.left is false
                                new Metadata().put("name", "Klaus").put("age", 666),

                                // And.left, And.right are both false
                                new Metadata().put("age", 666).put("name", "Alice"),
                                new Metadata())))

                // And: x2
                .add(Arguments.of(
                        and(
                                metadataKey("name").isEqualTo("Klaus"),
                                and(
                                        metadataKey("age").isEqualTo(42),
                                        metadataKey("city").isEqualTo("Munich"))),
                        asList(
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                // only And.left is present and true
                                new Metadata().put("name", "Klaus"),

                                // And.left is true, And.right is false
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata().put("name", "Klaus").put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 666)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Frankfurt"),

                                // only And.right is present and true
                                new Metadata().put("age", 42).put("city", "Munich"),

                                // And.right is true, And.left is false
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("name", "Alice"),
                                new Metadata())))
                .add(Arguments.of(
                        and(
                                and(
                                        metadataKey("name").isEqualTo("Klaus"),
                                        metadataKey("age").isEqualTo(42)),
                                metadataKey("city").isEqualTo("Munich")),
                        asList(
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                // only And.left is present and true
                                new Metadata().put("name", "Klaus").put("age", 42),

                                // And.left is true, And.right is false
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Frankfurt"),

                                // only And.right is present and true
                                new Metadata().put("city", "Munich"),

                                // And.right is true, And.left is false
                                new Metadata().put("city", "Munich").put("name", "Klaus"),
                                new Metadata()
                                        .put("city", "Munich")
                                        .put("name", "Klaus")
                                        .put("age", 666),
                                new Metadata().put("city", "Munich").put("age", 42),
                                new Metadata()
                                        .put("city", "Munich")
                                        .put("age", 42)
                                        .put("name", "Alice"),
                                new Metadata())))

                // === AND + nested OR ===

                .add(Arguments.of(
                        and(
                                metadataKey("name").isEqualTo("Klaus"),
                                or(
                                        metadataKey("age").isEqualTo(42),
                                        metadataKey("city").isEqualTo("Munich"))),
                        asList(
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("country", "Germany"),
                                new Metadata().put("name", "Klaus").put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("city", "Munich")
                                        .put("country", "Germany"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                // only And.left is present and true
                                new Metadata().put("name", "Klaus"),

                                // And.left is true, And.right is false
                                new Metadata().put("name", "Klaus").put("age", 666),
                                new Metadata().put("name", "Klaus").put("city", "Frankfurt"),

                                // only And.right is present and true
                                new Metadata().put("age", 42),
                                new Metadata().put("city", "Munich"),
                                new Metadata().put("age", 42).put("city", "Munich"),

                                // And.right is true, And.left is false
                                new Metadata().put("age", 42).put("name", "Alice"),
                                new Metadata().put("city", "Munich").put("name", "Alice"),
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("name", "Alice"),
                                new Metadata())))
                .add(Arguments.of(
                        and(
                                or(
                                        metadataKey("name").isEqualTo("Klaus"),
                                        metadataKey("age").isEqualTo(42)),
                                metadataKey("city").isEqualTo("Munich")),
                        asList(
                                new Metadata().put("name", "Klaus").put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("city", "Munich")
                                        .put("country", "Germany"),
                                new Metadata().put("age", 42).put("city", "Munich"),
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                // only And.left is present and true
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("age", 42),
                                new Metadata().put("name", "Klaus").put("age", 42),

                                // And.left is true, And.right is false
                                new Metadata().put("name", "Klaus").put("city", "Frankfurt"),
                                new Metadata().put("age", 42).put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Frankfurt"),

                                // only And.right is present and true
                                new Metadata().put("city", "Munich"),

                                // And.right is true, And.left is false
                                new Metadata().put("city", "Munich").put("name", "Alice"),
                                new Metadata().put("city", "Munich").put("age", 666),
                                new Metadata()
                                        .put("city", "Munich")
                                        .put("name", "Alice")
                                        .put("age", 666),
                                new Metadata())))

                // === OR + nested AND ===
                .add(Arguments.of(
                        or(
                                metadataKey("name").isEqualTo("Klaus"),
                                and(
                                        metadataKey("age").isEqualTo(42),
                                        metadataKey("city").isEqualTo("Munich"))),
                        asList(
                                // only Or.left is present and true
                                new Metadata().put("name", "Klaus"),
                                new Metadata().put("name", "Klaus").put("country", "Germany"),

                                // Or.left is true, Or.right is false
                                new Metadata().put("name", "Klaus").put("age", 666),
                                new Metadata().put("name", "Klaus").put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 666)
                                        .put("city", "Frankfurt"),

                                // only Or.right is present and true
                                new Metadata().put("age", 42).put("city", "Munich"),
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany"),

                                // Or.right is true, Or.left is false
                                new Metadata()
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("name", "Alice")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("age", 666),
                                new Metadata().put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Alice")
                                        .put("age", 666)
                                        .put("city", "Frankfurt"),
                                new Metadata())))
                .add(Arguments.of(
                        or(
                                and(
                                        metadataKey("name").isEqualTo("Klaus"),
                                        metadataKey("age").isEqualTo(42)),
                                metadataKey("city").isEqualTo("Munich")),
                        asList(
                                // only Or.left is present and true
                                new Metadata().put("name", "Klaus").put("age", 42),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("country", "Germany"),

                                // Or.left is true, Or.right is false
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Frankfurt"),

                                // only Or.right is present and true
                                new Metadata().put("city", "Munich"),
                                new Metadata().put("city", "Munich").put("country", "Germany"),

                                // Or.right is true, Or.left is true
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich"),
                                new Metadata()
                                        .put("name", "Klaus")
                                        .put("age", 42)
                                        .put("city", "Munich")
                                        .put("country", "Germany")),
                        asList(
                                new Metadata().put("name", "Alice"),
                                new Metadata().put("age", 666),
                                new Metadata().put("city", "Frankfurt"),
                                new Metadata()
                                        .put("name", "Alice")
                                        .put("age", 666)
                                        .put("city", "Frankfurt"),
                                new Metadata())))
                .build();
    }
}
