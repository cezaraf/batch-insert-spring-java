package dev.cezar.batch.postgresql.service;// MyBatchRepository.java

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class BatchInsertRepository {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public BatchInsertRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void insertAll(List<Map<String, Object>> data) {
        final var batchSize = 5_000;

        var sql = "INSERT INTO json_table (id, payload) VALUES (?, ?::jsonb)";

        for (var start = 0; start < data.size(); start += batchSize) {
            var end = Math.min(start + batchSize, data.size());
            var batch = data.subList(start, end);
            final int jump = start;
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    var row = batch.get(i);
                    var index = i + jump;
                    ps.setLong(1, index);
                    try {
                        String json = objectMapper.writeValueAsString(row);
                        ps.setString(2, json);
                    } catch (Exception e) {
                        throw new SQLException("Erro ao converter para JSON", e);
                    }
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });
        }
    }
}
