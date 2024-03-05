package com.echo.app.func;

import com.echo.utils.KeyWordUtil;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str){

        try {
            List<String> list = KeyWordUtil.splitKeyWord(str);
            for (String s : list) {
                collect(Row.of(s));
            }
        } catch (IOException e) {
            //切不出来就当作整体输出
            e.printStackTrace();
            collect(Row.of(str));
        }
    }

}
