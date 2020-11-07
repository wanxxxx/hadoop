package com.fangxi.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class sort {
    public static void main(String[] args) throws IOException {
        InputStream inputStream = new FileInputStream("C:/Users/fangxi/Desktop/work.txt");
        HashMap<String, Double> zeromap = new HashMap<String, Double>();
        Map<String, Double> Kmap = new HashMap<>();
        Map<String, Double> Mmap = new HashMap<>();
        Map<String, Double> Gmap = new HashMap<>();
        Scanner s = new Scanner(inputStream);
        s.nextLine();
        s.nextLine();
        while (s.hasNext()) {
            Double count = s.nextDouble();
            String tmp = s.next();
            if (tmp.charAt(0) == '/') {
                zeromap.put(tmp, 0.0);
            } else if (tmp.charAt(0) == 'K') {
                Kmap.put(s.next(), count);
            } else if (tmp.charAt(0) == 'M') {
                Mmap.put(s.next(), count);
            } else if (tmp.charAt(0) == 'G') {
                Gmap.put(s.next(), count);
            }
        }

        List<String> KList = Kmap.entrySet().stream()
                .sorted((Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) -> (int)(e2.getValue() - e1.getValue()))
                .map(entry -> entry.getKey()).collect(Collectors.toList());
        List<String> MList = Mmap.entrySet().stream()
                .sorted((Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) -> (int)(e2.getValue() - e1.getValue()))
                .map(entry -> entry.getKey()).collect(Collectors.toList());
        List<String> GList = Gmap.entrySet().stream()
                .sorted((Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) -> (int)(e2.getValue() - e1.getValue()))
                .map(entry -> entry.getKey()).collect(Collectors.toList());

        System.out.println("1M内占用空间最多的是："+groovy.json.JsonOutput.toJson(KList.get(0))+" 为 "+groovy.json.JsonOutput.toJson(Kmap.get(KList.get(0)))+"K");
        System.out.println("1G内占用空间最多的是："+groovy.json.JsonOutput.toJson(MList.get(0))+" 为 "+groovy.json.JsonOutput.toJson(Mmap.get(MList.get(0)))+"M");
        System.out.println("1T内占用空间最多的是："+groovy.json.JsonOutput.toJson(GList.get(0))+" 为 "+groovy.json.JsonOutput.toJson(Gmap.get(GList.get(0)))+"G");
        inputStream.close();
    }
}

