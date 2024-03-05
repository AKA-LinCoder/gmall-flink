package com.echo.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

//iKUN分词器
public class KeyWordUtil {
    public static List<String> splitKeyWord(String keyWord) throws IOException {
        //创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();
        //创建分词对象
        StringReader stringReader = new StringReader(keyWord);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,false);
        Lexeme next = ikSegmenter.next();
        //循环取出切好的词
        while (next != null){

            String text = next.getLexemeText();
            list.add(text);
            next = ikSegmenter.next();
        }


        return list;
    }
}
