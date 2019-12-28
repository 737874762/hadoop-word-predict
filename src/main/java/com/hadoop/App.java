package com.hadoop;


import com.hadoop.response.CommonreturnType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;


/**
 * Hello world!
 *
 */
@SpringBootApplication(scanBasePackages = {"com.hadoop"})
public class App
{


    @GetMapping("/")
    public CommonreturnType home(){
        return CommonreturnType.create("连接正常");
    }

    public static void main( String[] args )
    {
        SpringApplication.run(App.class,args);
    }
}
