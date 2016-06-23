package com.flink.config;

import com.flink.entry.Blog;
import com.flink.repository.BlogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DataInit {
    
    @Autowired
    BlogRepository blogRepository;
    
    //@PostConstruct
    public void data(){
        for(Integer i = 0; i < 123; i++){
            Blog blog = new Blog();
            blog.setContent("this is blog content");
            blog.setTitle("blog" + i);
            blog = blogRepository.save(blog);
        }
    }

}
