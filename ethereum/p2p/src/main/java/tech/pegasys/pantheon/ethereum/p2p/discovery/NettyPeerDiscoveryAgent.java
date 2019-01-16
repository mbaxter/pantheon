package tech.pegasys.pantheon.ethereum.p2p.discovery;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.Packet;
import tech.pegasys.pantheon.ethereum.p2p.discovery.internal.TimerUtil;
import tech.pegasys.pantheon.ethereum.p2p.peers.Endpoint;

public class NettyPeerDiscoveryAgent extends PeerDiscoveryAgent {

  private final EventLoopGroup boss = new NioEventLoopGroup(1);
  private final EventLoopGroup workers = new NioEventLoopGroup(1);
  private ChannelFuture server;

  @Override
  protected TimerUtil createTimer() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CompletableFuture<InetSocketAddress> listenForConnections() {
    server =
      new /*Server*/Bootstrap()
        /*.group(boss, workers)*/
        .channel(NioDatagramChannel.class)
        .handler(createChannelInitializer())
        .bind(config.getBindHost(), config.getBindPort());

        server.addListener((future) -> {
          InetSocketAddress localAddress = (InetSocketAddress) server.channel().localAddress();
          LOG.info(
            "Started peer discovery agent successfully, on effective host={} and port={}",
            localAddress.getHostString(),
            localAddress.getPort());
        });
  }

  private ChannelInitializer<NioDatagramChannel> createChannelInitializer() {
    return new ChannelInitializer<NioDatagramChannel>() {
      @Override
      protected void initChannel(NioDatagramChannel ch) throws Exception {
        ch.pipeline()
          .addLast(decoder())
          .addLast(packetHandler());
      }
    };
  }

  private ChannelInboundHandlerAdapter decoder() {
    return new ByteToMessageDecoder() {
      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        throws Exception {
        Packet packet = Packet.decode(in);
        while(packet != null) {
          out.add(packet);
          packet = Packet.decode(in);
        }
      }
    };
  }

  private SimpleChannelInboundHandler<Packet> packetHandler() {
    return new SimpleChannelInboundHandler<Packet>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        InetSocketAddress peerAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        Endpoint peerEndpoint = new Endpoint(peerAddress.getHostName(), peerAddress.getPort(),
          OptionalInt.empty());
        handleIncomingPacket(peerEndpoint, packet);
      }
    };
  }


  @Override
  protected CompletableFuture<Void> sendOutgoingPacket(DiscoveryPeer peer,
    Packet packet) {

  }

  @Override
  public CompletableFuture<?> stop() {
    throw new UnsupportedOperationException();
  }
}
