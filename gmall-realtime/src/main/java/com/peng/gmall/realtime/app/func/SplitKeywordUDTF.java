package com.peng.gmall.realtime.app.func;

import com.peng.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author bp
 * @create 2021-07-07 20:03
 */

//添加注解定义输出类型
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitKeywordUDTF extends TableFunction<Row> {
    public void eval(String keyword) {
        //进行切分
        List<String> splitKeyword = KeywordUtil.splitKeyword(keyword);
        //遍历切分的单词
        for (String word : splitKeyword) {
            collect(Row.of(word));
        }
    }
}
