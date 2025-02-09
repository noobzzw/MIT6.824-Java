/*
 * RpcClient.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package rpc.io;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import rpc.common.RpcEncoder;
import rpc.common.RpcEncoder.JSONRpcSerializer;
import rpc.common.RpcRequest;
import util.LogUtil;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RpcClient {

    private final ExecutorService executorService = Executors
        .newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public Object call(int port, String methodName) {
        return call(port,methodName,null);
    }


    /**
     * 进行方法调用
     * @param port service port
     * @param methodName 调用的方法
     * @param args 方法入参
     * @return Future，异步调用
     */
    @SuppressWarnings(value = {"unchecked", "rawtypes"})
    public Future call(int port, String methodName, Object[] args) {
        RpcRequest request = new RpcRequest();
        request.setRequestId(UUID.randomUUID().toString());
        request.setMethodName(methodName);
        request.setParameters(args);
        Class<?>[] parameterTypes = new Class[0];
        if (null != args) {
            parameterTypes = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                parameterTypes[i] = args[i].getClass();
            }
        }
        request.setParameterTypes(parameterTypes);
        RpcChannel rpcChannel = new RpcChannel(port);
        rpcChannel.setRequest(request);
        try {
            bind(rpcChannel);
            return executorService.submit(rpcChannel);
        } catch (Exception e) {
            LogUtil.log("fail to call ");
        }
        return null;
    }


    private void bind(RpcChannel rpcChannel) throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addFirst(new RpcEncoder(RpcRequest.class, new JSONRpcSerializer()));
                    pipeline.addLast(new StringDecoder());
                    pipeline.addLast(rpcChannel);
                }
            });
        bootstrap.connect("127.0.0.1", rpcChannel.getServerPort()).sync();
    }
}
