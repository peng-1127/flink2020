package com.peng.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bp
 * @create 2021-07-07 19:46
 */
public class KeywordUtil {
    //切完词之后将一个一个单词放入集合
    public static List<String> splitKeyword(String keyWord) {
        //创建Reader对象
        StringReader reader = new StringReader(keyWord);

        //创建IK分词器对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        //创建集合用于存放切分以后的关键词
        ArrayList<String> words = new ArrayList<>();

        Lexeme next = null;
        try {
            next = ikSegmenter.next();

            //如果切词的下一个不为null,就一直取下一个
            while (next != null) {
                //将切分的关键词放入集合
                words.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //返回结果
        return words;
    }

    public static void main(String[] args) {
        System.out.println(splitKeyword("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待"));
    }
}
