package com.flink.controller;

import com.flink.cep.LogForCEP;
import com.flink.entry.LogEntity;
import com.flink.entry.Parma;
import com.flink.repository.LogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by jaryzhen on 5/25/16.
 */
@RestController
@RequestMapping("/log")
public class LogController {


    @Autowired
    LogRepository logRepository;

    // http://localhost:8080/log/sev?page=xxx&size=xxx&search=xxx&logsize=100000&uid=XXX&transaction=start_SendThirdPartWorker_done:20&contains=xxx
    //http://10.128.32.111:8080/log/sev?page=0&size=150&search=*_and_timestamp=2016-06-18T04:06:30.653Z_2016-06-20T04:11:30.654Z&logsize=100000&uid=all&transaction=start_SendThirdPartWorker_done:20&contains=null

    @RequestMapping(value = "/sev ", method = RequestMethod.GET)
    public Page<LogEntity> sev(//分页参数
                               @RequestParam(value = "page", defaultValue = "0") Integer page,
                               @RequestParam(value = "size", defaultValue = "10") Integer size,

                               //获取数据
                               @RequestParam(value = "from", required = true, defaultValue = "0") String from,
                               @RequestParam(value = "logsize", required = true, defaultValue = "100000") Long logsize,
                               @RequestParam(value = "search", required = true, defaultValue = "*") String search,
                               //transaction 结果for one id
                               @RequestParam(value = "uid", required = true, defaultValue = "all") String uid,
                               //transaction
                               @RequestParam(value = "transaction", required = true, defaultValue = "groupby_start_SendThirdPartWorker_done:20") String transaction,
                               //2016-06-08T02:14:51.069Z_016-06-08T02:19:51.069Z
                               @RequestParam(value = "contains", required = true, defaultValue = "null") String contains

    ) throws Exception {

        //处理请求参数
        Parma parma = new Parma();
        parma.setSearch(search);
        parma.setFrom(from);
        parma.setLogsize(logsize);
        parma.setUid(uid);
        parma.setContains(contains);
        logRepository.deleteAll();
        //transaction="start_SendThirdPartWorker_done:20";
        if (transaction.contains(":")) {
            int within = Integer.parseInt(transaction.split(":")[1]);
            String[] tss = transaction.split(":")[0].split("_");
            System.out.print("tss......"+tss.length);
            if (tss.length<4){
                LogEntity blog = new LogEntity();
                blog.setMessage("tranaction must contains <groupby startwith followby endwith >");
                //blogRepository.delete(blog);
                blog = logRepository.save(blog);
                Sort sort = new Sort(Sort.Direction.DESC, "id");
                Pageable pageable = new PageRequest(page, size, sort);
                return logRepository.findAll(pageable);
            }
            parma.setTransaction(tss);
            parma.setWithin(within);
        } else {
            String[] tss = transaction.split(":")[0].split("_");
            System.out.print("tss......"+tss.length);
            if (tss.length<4){
                LogEntity blog = new LogEntity();
                blog.setMessage("tranaction must contains <groupby startwith followby endwith>");
                //blogRepository.delete(blog);
                blog = logRepository.save(blog);
                Sort sort = new Sort(Sort.Direction.DESC, "id");
                Pageable pageable = new PageRequest(page, size, sort);
                return logRepository.findAll(pageable);
            }
            parma.setTransaction(tss);
            parma.setWithin(20);
        }

        List<LogEntity> listall = LogForCEP.getLog(parma);
        System.out.println("getlog size :" + listall.size());

        System.out.println("logRepository before" + logRepository.count());

        System.out.println("logRepository after delete" + logRepository.count());
        for (LogEntity log : listall) {
            log = logRepository.save(log);
        }


        //datasavel();

        System.out.println("logRepository after all " + logRepository.count());

        Sort sort = new Sort(Sort.Direction.DESC, "id");
        Pageable pageable = new PageRequest(page, size, sort);
        return logRepository.findAll(pageable);
    }

    //    @PostConstruct
    public void datasavel() {
        logRepository.deleteAll();
        for (Integer i = 0; i < 323; i++) {
            LogEntity blog = new LogEntity();
            blog.setIndex("this is blog content");
            blog.setMessage("blog" + i);
            //blogRepository.delete(blog);
            blog = logRepository.save(blog);
        }
    }
}
