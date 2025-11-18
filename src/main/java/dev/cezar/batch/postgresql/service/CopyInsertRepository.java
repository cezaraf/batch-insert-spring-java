package dev.cezar.batch.postgresql.service;// MyCopyRepository.java

import org.postgresql.PGConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class CopyInsertRepository {

    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    public CopyInsertRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.dataSource = jdbcTemplate.getDataSource();
        this.objectMapper = objectMapper;
    }

    public void copyInsertAllInBatches(List<Map<String, Object>> data) throws Exception {
        final var copyBatchSize = 50_000;

        try (var conn = dataSource.getConnection()) {
            var pgConnection = conn.unwrap(PGConnection.class);
            var copyManager = pgConnection.getCopyAPI();

            var sqlCopy = "COPY json_table (id, payload) FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '\"')";

            for (var start = 0; start < data.size(); start += copyBatchSize) {
                var end = Math.min(start + copyBatchSize, data.size());
                var batch = data.subList(start, end);

                var sb = new StringBuilder();

                for (int batchIndex = 0; batchIndex < batch.size(); batchIndex++) {
                    var row = batch.get(batchIndex);
                    var json = objectMapper.writeValueAsString(row).replace("\"", "\"\"");
                    sb.append(batchIndex + start)
                            .append(';')
                            .append('"')
                            .append(json)
                            .append('"')
                            .append('\n');
                }

                try (var input = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8))) {
                    var inserted = copyManager.copyIn(sqlCopy, input);
                    System.out.println("COPY batch inserted: " + inserted);
                }
            }
        }
    }
}
