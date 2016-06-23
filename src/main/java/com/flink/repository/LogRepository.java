package com.flink.repository;



import com.flink.entry.LogEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface LogRepository extends PagingAndSortingRepository<LogEntity, Integer> {
    
    Page<LogEntity> findByDeletedFalse(Pageable pageable);

}
