package chapter4;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.util.HashMap;

public class ParameterToolDemo {
    // main()方法——Java应用的入口
    public static void main(String[] args) throws Exception {
        /*
        从 Map 中读取参数
        */
        HashMap properties = new HashMap();
        // 配置bootstrap.servers的地址和端口
        properties.put("bootstrap.servers","127.0.0.1.9092");
        // 配置Zookeeper的地址和端口
        properties.put("zookeeper.connect","127.0.0.1.2181");
        properties.put("topic","myTopic");


        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        System.out.println(parameterTool.getRequired("topic"));
        System.out.println(parameterTool.getProperties());

        /*
         从.porperties files文件中读取参数
         */
        String propertiesFilePath = "src/main/resources/myjob.properties";
        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);
        System.out.println(parameter.getRequired("my"));
        System.out.println(parameter.getProperties());
        File propertiesFile = new File(propertiesFilePath);
        ParameterTool parameterFile = ParameterTool.fromPropertiesFile(propertiesFile);
        System.out.println(parameterFile.getProperties());
        System.out.println(parameterFile.getRequired("my"));

        /*
        从命令行中读取参数
         */
        ParameterTool parameterfromArgs = ParameterTool.fromArgs(args);
        System.out.println("parameterfromArgs:"+parameterfromArgs.getProperties());

        /*
        从系统配置中读取参数
         */
        ParameterTool parameterfromSystemProperties = ParameterTool.fromSystemProperties();
        System.out.println("parameterfromSystemProperties:"+parameterfromSystemProperties.getProperties());

    }
}
