package com.hnxy.mr;

import com.alibaba.fastjson.JSON;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class top10 {
    public static void main(String[] args) {

        try {
            InputStream inputStream = new FileInputStream("C:/Users/fangxi/Desktop/test_count/file3");
            Map<String, Integer> words = new HashMap<>();
            Scanner s = new Scanner(inputStream);
            while (s.hasNext()) {
                String word = s.next();
                if (!words.containsKey(word)) {
                    words.put(word, 1);
                } else {
                    words.compute(word, (key, value) -> value + 1);
                }

            }
            List<String> mobileList = words.entrySet().stream()
                    .sorted((Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> e2.getValue() - e1.getValue())
                    .map(entry -> entry.getKey()).collect(Collectors.toList())
                    ;
            System.out.println(JSON.toJSONString(mobileList));

            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
