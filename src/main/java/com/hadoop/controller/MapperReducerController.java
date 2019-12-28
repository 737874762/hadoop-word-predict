package com.hadoop.controller;

import com.hadoop.pojo.MyResult;

import com.hadoop.pojo.PredictResult;
import com.hadoop.response.CommonreturnType;
import io.swagger.annotations.Api;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


/**
 * 〈〉
 *
 * @author Chkl
 * @create 2019/12/24
 * @since 1.0.0
 */
@RestController
@RequestMapping("/api/mr")
@Api(description = "评价预测模块")
@CrossOrigin(allowCredentials = "true", allowedHeaders = "*") //解决跨域问题
public class MapperReducerController {

    List<PredictResult> predictResults = new ArrayList<>();
    List<Double> infos = new ArrayList<>();

    @PostMapping(value = "/upLoadFile")
    public CommonreturnType upLoadFile(@RequestParam(value = "file") MultipartFile file) throws Exception {

        /**
         * 将上传到代码平台的代码上传到HDFS
         */
        //将文件从浏览器端上传到服务器端
        String fileUrl = uploadFile(file);
        System.out.println(fileUrl);
        //将文件从服务器端传输到hadoop
        fileUpLoadToHdfs(fileUrl);
        //进行预测生成文件
//        new PredictApp();
        return CommonreturnType.create(200);
    }

    /**
     * 将文件上传到HDFS
     *
     * @param filePath
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    private void fileUpLoadToHdfs(String filePath) throws URISyntaxException, IOException, InterruptedException {


        Configuration configuration = new Configuration();
//        设置副本数为1
        configuration.set("dfs.replication", "1");
        /**
         * 参数1：hdfs的uri
         * 参数2：客户端指定的配置参数
         * 参数3：客户端的身份，就是操作用户名
         */
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.199.200:8020"), configuration, "hadoop");
        Path src = new Path(filePath);
        Path dst = new Path("/predict_input_2017081119/test.txt");
        fileSystem.copyFromLocalFile(src, dst);
        configuration = null;
        fileSystem = null;
    }


    /**
     * 文件上传工具类
     * 将文件上传到部署平台
     *
     * @param file
     * @throws Exception
     */
    private String uploadFile(MultipartFile file) {

        String fileName = file.getOriginalFilename();
        //文件上传到本项目所在平台的某个路径下
        String filePath = "H:/WorkSpace/intellijWorkspace/hadoop-word-predict/upload/";

        try {
            File targetFile = new File(filePath);
            if (!targetFile.exists()) {
                targetFile.mkdirs();
            }
            FileOutputStream out = new FileOutputStream(filePath + fileName);
            out.write(file.getBytes());
            out.flush();
            out.close();
        } catch (Exception e) {

            e.printStackTrace();
            return null;
        }
        return filePath + fileName;
    }


    /**
     * 查询预测结果
     *
     * @return
     */
    @GetMapping(value = "/getall")
    public CommonreturnType getall() {
        predictResults.clear();
        infos.clear();
        readInfo();
        MyResult myResult = new MyResult();
        myResult.setPredictResults(predictResults);
        myResult.setGoodCount(infos.get(0));
        myResult.setBadCount(infos.get(1));
        myResult.setCorrect(infos.get(2));
        return CommonreturnType.create(myResult);
    }



    private void readInfo() {




        //    1.读取hdfs的文件 ==>HDFS API
        Path input = new Path("/predict_output_2017081119/part-r-00000");
        try {
            //获取hdfs文件系统
            FileSystem fs = null;

            fs = FileSystem.get(new URI("hdfs://192.168.199.200:8020"), new Configuration(), "hadoop");

            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);//不递归的获取文件

            while (iterator.hasNext()) {
                LocatedFileStatus file = iterator.next();
                FSDataInputStream in = fs.open(file.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line = "";
                while ((line = reader.readLine()) != null) {//读取到的行不为空

                    String[] split = line.split("\\s+");
                    if (split.length == 2 && Integer.parseInt(split[0]) != 2017081119) {

                        PredictResult predictResult = new PredictResult();
                        predictResult.setLineNum(split[0]);
                        predictResult.setPResult(split[1]);

                        if (Integer.parseInt(split[0]) <= 1000) {
                            predictResult.setTResult("好评");
                        } else {
                            predictResult.setTResult("差评");
                        }
                        predictResults.add(predictResult);
                    }
                    if (Integer.parseInt(split[0]) == 2017081119) {
                        //如果是最后三个统计参数，再以中文分号进行分割
                        String[] info = split[1].split("：");
                        infos.add(Double.parseDouble(info[1]));
                        System.out.println(info[1]);
                    }
                }
                reader.close();
                in.close();

            }

            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
