package dev.cezar.batch.postgresql;

import com.zaxxer.hikari.HikariDataSource;
import dev.cezar.batch.postgresql.service.BatchInsertRepository;
import dev.cezar.batch.postgresql.service.CopyInsertRepository;
import dev.cezar.batch.postgresql.service.JsonInsertService;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Json bulk insert integration test")
public class JsonBulkInsertIntegrationTest {

    private ObjectMapper objectMapper = new ObjectMapper();
    private String schema;

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("testdb")
                    .withUsername("test")
                    .withPassword("test");

    private JdbcTemplate jdbcTemplate;
    private JsonInsertService jsonInsertService;

    @BeforeAll
    void setup() {

        this.schema = UUID.randomUUID().toString();

        var dataSource = createDataSource();

        this.jdbcTemplate = new JdbcTemplate(dataSource);

        this.jdbcTemplate.execute("CREATE SCHEMA \"%s\"".formatted(this.schema));

        this.jdbcTemplate.execute(
                """
                        create table "%s".json_table (
                            id bigint primary key,
                            payload jsonb
                        )
                        """.formatted(this.schema)
        );

        var batchRepository = new BatchInsertRepository(this.schema, this.jdbcTemplate, this.objectMapper);

        var copyRepository = new CopyInsertRepository(this.schema, this.jdbcTemplate, this.objectMapper);

        this.jsonInsertService = new JsonInsertService(batchRepository, copyRepository);
    }

    private DataSource createDataSource() {
        var ds = new HikariDataSource();
        ds.setJdbcUrl(postgres.getJdbcUrl());
        ds.setUsername(postgres.getUsername());
        ds.setPassword(postgres.getPassword());
        return ds;
    }

    @BeforeEach
    void cleanTable() {
        this.jdbcTemplate.update("delete from \"%s\".json_table".formatted(this.schema));
    }

    @Test
    @DisplayName("Should insert using batch")
    void shouldInsertUsingBatch_WhenBelowThreshold() throws Exception {
        int size = 99_999;
        var data = generateData(size);

        this.jsonInsertService.insert(data);

        var count = this.jdbcTemplate.queryForObject("select count(*) from \"%s\".json_table".formatted(this.schema), Integer.class);
        assertThat(count).isEqualTo(size);

        var payload = this.jdbcTemplate.queryForObject("select payload from \"%s\".json_table where id = 0".formatted(this.schema), String.class);
        var json = this.objectMapper.readTree(payload);

        assertThat(json.isObject()).isTrue();
        assertThat(json.get("id").asInt()).isEqualTo(1);
        assertThat(json.get("name").asString()).isEqualTo("item-\"1\"");
    }

    @Test
    @DisplayName("Should insert using copy")
    void shouldInsertUsingCopy_WhenAboveThreshold() throws Exception {
        var size = 1_000_000;
        var data = generateData(size);

        this.jsonInsertService.insert(data);

        var count = this.jdbcTemplate.queryForObject("select count(*) from \"%s\".json_table".formatted(this.schema), Integer.class);
        assertThat(count).isEqualTo(size);

        var ids = this.jdbcTemplate.queryForList("select id from \"%s\".json_table where id in (1, 75_000, 150_000, 999_999)".formatted(this.schema), Long.class);

        assertThat(ids).contains(1L, 75_000L, 150_000L, 999_999L);

        var payload = this.jdbcTemplate.queryForObject("select payload from \"%s\".json_table where id = 0".formatted(this.schema), String.class);
        var json = this.objectMapper.readTree(payload);

        assertThat(json.isObject()).isTrue();
        assertThat(json.get("id").asInt()).isEqualTo(1);
        assertThat(json.get("name").asString()).isEqualTo("item-\"1\"");
    }

    private List<Map<String, Object>> generateData(int size) {
        return LongStream.rangeClosed(1, size)
                .mapToObj(id -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", id);
                    map.put("name", "item-\"" + id + "\"");
                    map.put("value", id * 10);
                    return map;
                })
                .collect(Collectors.toList());
    }
}
