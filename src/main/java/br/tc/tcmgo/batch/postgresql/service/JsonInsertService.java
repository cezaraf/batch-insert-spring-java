package br.tc.tcmgo.batch.postgresql.service;

import java.util.List;
import java.util.Map;

public class JsonInsertService {

    private static final int COPY_THRESHOLD = 100_000;

    private final BatchInsertRepository batchRepository;
    private final CopyInsertRepository copyRepository;

    public JsonInsertService(BatchInsertRepository batchRepository,
                             CopyInsertRepository copyRepository) {
        this.batchRepository = batchRepository;
        this.copyRepository = copyRepository;
    }

    public void insert(List<Map<String, Object>> data) throws Exception {
        if (data.size() <= COPY_THRESHOLD) {
            batchRepository.insertAll(data);
        } else {
            copyRepository.copyInsertAllInBatches(data);
        }
    }
}
