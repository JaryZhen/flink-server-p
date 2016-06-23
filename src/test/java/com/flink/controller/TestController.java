package com.flink.controller;

import com.flink.entry.Blog;
import com.flink.repository.BlogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
    
    @Autowired
    BlogRepository blogRepository;
    
    @RequestMapping(value = "/params", method=RequestMethod.GET)
    public Page<Blog> getEntryByParams(
            @RequestParam(value = "page", defaultValue = "0") Integer page,
            @RequestParam(value = "size", defaultValue = "15") Integer size) {
        data();
        Sort sort = new Sort(Direction.DESC, "id");
        Pageable pageable = new PageRequest(page, size, sort);
        return blogRepository.findAll(pageable);
    }
    
    @RequestMapping(value = "sd", method=RequestMethod.GET)
    public Page<Blog> getEntryByPageable
            (@PageableDefault(value = 15, sort = { "id" }, direction = Direction.DESC) Pageable pageable) {
        data();
        return blogRepository.findAll(pageable);
    }

    public void data(){
        for(Integer i = 0; i < 123; i++){
            Blog blog = new Blog();
            blog.setContent("this is blog content");
            blog.setTitle("blog" + i);
            blog = blogRepository.save(blog);
        }
    }
}
