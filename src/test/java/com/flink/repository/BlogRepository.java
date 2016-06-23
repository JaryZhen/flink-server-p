package com.flink.repository;

import com.flink.entry.Blog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface BlogRepository extends PagingAndSortingRepository<Blog, Integer> {
    
    Page<Blog> findByDeletedFalse(Pageable pageable);

}
