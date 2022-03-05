/*
 * NettyServer.java

 */

package rpc.io;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import java.lang.reflect.Method;
import java.util.Arrays;

import rpc.common.RpcDecoder;
import rpc.common.RpcEncoder.JSONRpcSerializer;
import rpc.common.RpcRequest;
import util.LogUtil;

public class RpcServer {

    /**
     * 启动这个 RPC 的网络服务
     * @throws Exception
     */
    public void serve(int port) throws Exception {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(bossGroup, workGroup);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.childHandler(new ChannelInitializer<NioSocketChannel>() {
            @Override
            protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
                ChannelPipeline pipeline = nioSocketChannel.pipeline();
                pipeline.addFirst(new StringEncoder());
                pipeline.addLast(new RpcDecoder(RpcRequest.class, new JSONRpcSerializer()));
                pipeline.addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object o)
                        throws Exception {
                        ctx.writeAndFlush(invoke(o));
                    }
                });
            }
        });
        serverBootstrap.bind(port).sync();
        LogUtil.log("server started on port: " + port);
    }

    private Object invoke(Object o) throws Exception {
        if (!(o instanceof RpcRequest)) {
            throw new Exception("request type isn't match RpcRequest");
        }
        RpcRequest rpcRequest = (RpcRequest) o;
        Object serverObj = this;
        Class<?> serverClass = serverObj.getClass();
        String methodName = rpcRequest.getMethodName();
        Class<?>[] parameterTypes = rpcRequest.getParameterTypes();
        // 存储反序列化后的参数
        Object[] parameters = new Object[0];
        // 通过反射调用方法
        Method method;
        if (parameterTypes.length == 0) {
            // 无参调用
            method = serverClass.getDeclaredMethod(methodName);
        } else {
            // 有参调用
            method = serverClass.getDeclaredMethod(methodName, parameterTypes);
            // 处理传入参数，因为统一采用fastjson进行序列化，这里需要将json反序列化
            parameters = new Object[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                final String parameter = rpcRequest.getParameters()[i].toString();
                parameters[i] = JSON.parseObject(parameter,parameterTypes[i]);
            }
        }
        method.setAccessible(true);
        // 调用
        return method.invoke(serverObj,parameters);
    }
}
