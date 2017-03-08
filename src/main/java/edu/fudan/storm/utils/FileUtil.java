package edu.fudan.storm.utils;

import java.io.File;

/**
 * Created by ybwang on 3/2/17.
 */
public class FileUtil {
    public static void createFile(String path){
        File file = new File(path);
        file.mkdirs();
    }
    public static void main(String[] args){
        createFile("/home/ybwang/testPath/testFile/test");
    }

}
