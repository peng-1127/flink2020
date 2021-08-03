package com.atguigu.gmalllogger.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author bp
 * @create 2021-06-24 15:07
 */

//@Controller
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    //    @ResponseBody
    @RequestMapping("/test01")
    public String test1() {
        System.out.println("test1");
        return "success";
    }

    @RequestMapping("/test02")
    public String test02(
            @RequestParam("name") String name,
            @RequestParam(value = "age", defaultValue = "18") int age
    ) {
        System.out.println(name + ":" + age);
        return "success";
    }


    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //落盘
        log.info(jsonStr);


        //写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);


        return "success";
    }


}
