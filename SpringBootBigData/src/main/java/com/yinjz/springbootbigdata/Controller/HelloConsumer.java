package com.yinjz.springbootbigdata.Controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloConsumer {
    @RequestMapping("/hello")
    public String data(String msg) {
        return "Hello " + msg ;
    }
}
