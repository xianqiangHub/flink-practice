package com.bigdata;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 定义一个CommonSinkOperator的抽象类，继承AbstractStreamOperator，并且实现ProcessingTimeCallback与OneInputStreamOperator接口，那么看下类里面的方法，
 * <p>
 * 首先看构造函数传入批写大小batchSize与定时写入时间interval
 * <p>
 * 重载了AbstractStreamOperator的open方法，在这个方法里面获取ProcessingTimeService，然后注册一个interval时长的定时器
 * <p>
 * 重载AbstractStreamOperator的initializeState方法，用于恢复内存数据
 * <p>
 * 重载AbstractStreamOperator的snapshotState方法，在checkpoint会将内存数据写入状态中容错
 * <p>
 * 实现了OneInputStreamOperator接口的processElement方法，将结果数据写入到内存中，如果满足一定大小则输出
 * <p>
 * 实现了ProcessingTimeCallback接口的onProcessingTime方法，注册定时器的执行方法，进行数据输出的同时，注册下一个定时器
 * <p>
 * 定义了一个抽象saveRecords方法，实际输出操作
 * <p>
 * <p>
 * <p>
 * 那么这个CommonSinkOperator就是一个模板方法，能够做到将任何类型的数据输出，只需要继承该类，并且实现saveRecords方法即可。
 * 自定义
 *
 * @param <T>
 */
public abstract class CommonSinkOperate<T extends Serializable, Object> extends AbstractStreamOperator<Object> implements ProcessingTimeCallback, OneInputStreamOperator<T, Object> {

    private List<T> list;
    private ListState<T> listState;
    private int batchSize;
    private Long inteval;
    private ProcessingTimeService processingTimeService;

    public CommonSinkOperate() {
    }

    public CommonSinkOperate(int batchSize, Long inteval) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = batchSize;
        this.inteval = inteval;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (inteval > 0 && batchSize > 1) {
            processingTimeService = getProcessingTimeService();
            long now = processingTimeService.getCurrentProcessingTime();
            processingTimeService.registerTimer(now + inteval, this);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.list = new ArrayList<>();
        listState = context.getOperatorStateStore().getSerializableListState("batch-interval-sink");

        if (context.isRestored()) {
            for (T x : listState.get()) {
                list.add(x);
            }
        }
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {

        list.add(element.getValue());
        if (list.size() >= batchSize) {
            saveRecords(list);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (list.size() > 0) {
            listState.clear();
            listState.addAll(list);
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        if (list.size() > 0) {
            saveRecords(list);
            list.clear();
        }

        long now = processingTimeService.getCurrentProcessingTime();
        processingTimeService.registerTimer(now + inteval, this);
    }

    public abstract void saveRecords(List<T> datas);
}
