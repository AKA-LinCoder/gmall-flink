package com.echo.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 在map算子中计算数据的个数
 * 算子状态中，list和unionList的区别：并行度改变，怎么重新分配状态
 * list：轮询均分
 * unionlist:原先的多个子任务的状态，合并成一个完整的，给新的并行子任务广播
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("hadoop102",7777).map(new MyCountMapFunction()).print();


        environment.execute();
    }

    //TODO 1 实现checkpointFunction 接口
    public static class MyCountMapFunction implements MapFunction<String,Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> listState;

        @Override
        public Long map(String s) throws Exception {
            return ++count;
        }

        /**
         * 对状态做快照，定义将本地变量拷贝到算子状态中
         * @param functionSnapshotContext
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

            System.out.println("调用一次快照");
            //TODO 2 获取算子状态
            listState.clear();;
            listState.add(count);
        }

        /**
         * 初始化本地变量，从状态中，将数据添加到本地变量，每个子任务调用一次,用在程序恢复时
         * @param functionInitializationContext
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            System.out.println("恢复中");
            //从上下文中获取算子状态
           listState = functionInitializationContext.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("state", Types.LONG));
           if(functionInitializationContext.isRestored()){
               for (Long c : listState.get()) {
                   count += c;
               }
           }
        }
    }

}
