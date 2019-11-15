package com.bigdata;

/**
 * /**
 * 流的关联有
 * JoinedStream & CoGroupedStreams
 * JoinedStreams在底层又调用了CoGroupedStream来实现Join功能。
 * 实际上着两者还是有区别的，首先co-group侧重的是group,是对同一个key上的两组集合进行操作，
 * 而join侧重的是pair,是对同一个key上的每对元素操作。
 * co-group比join更通用一些，因为join只是co-group的一个特例，所以join是可以基于co-group来实现的（当然有优化的空间）。
 * 而在co-group之外又提供了join接口是因为用户更熟悉join
 * <p>
 * connect和union
 * ①.ConnectedStreams只能连接两个流，而union可以多余两个流；
 * ②.ConnectedStreams连接的两个流类型可以不一致，而union连接的流的类型必须一致；
 * ③.ConnectedStreams会对两个流的数据应用不同的处理方法，并且双流之间可以共享状态。
 * 这再第一个流的输入会影响第二流时，会非常有用。
 *
 *  相同的key再一台机器上处理，可能会有数据倾斜
 */
public class JoinMain {

    public static void main(String[] args) {


    }
}
