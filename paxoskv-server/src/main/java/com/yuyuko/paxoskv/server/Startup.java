package com.yuyuko.paxoskv.server;

import com.yuyuko.paxoskv.core.utils.Tuple;
import com.yuyuko.paxoskv.remoting.peer.PeerMessageProcessor;
import com.yuyuko.paxoskv.remoting.peer.PeerNode;
import com.yuyuko.paxoskv.remoting.peer.server.NettyPeerServerConfig;
import com.yuyuko.paxoskv.remoting.server.ClientRequestProcessor;
import com.yuyuko.paxoskv.server.core.PaxosNode;
import com.yuyuko.paxoskv.server.core.Server;
import com.yuyuko.paxoskv.server.statemachine.StateMachine;
import com.yuyuko.paxoskv.server.utils.Triple;
import com.yuyuko.selector.Channel;
import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Startup {
    public static void main(String[] args) throws ParseException {
        Triple<Long, Integer, List<PeerNode>> idPortPeerNodesTriple = parseCmdLine("raftkv", args);

        Server server = createServer(
                idPortPeerNodesTriple.getFirst(),
                idPortPeerNodesTriple.getSecond(),
                idPortPeerNodesTriple.getThird());
        server.start();
    }

    private static Server createServer(long id, int port, List<PeerNode> peerNodes) {

        Tuple<PeerMessageProcessor, ClientRequestProcessor> tuple =
                PaxosNode.newPaxosNode(id,
                        peerNodes.stream().map(PeerNode::getId).collect(Collectors.toList()));

        return new Server(id, port, tuple.getSecond(), peerNodes, tuple.getFirst());
    }


    private static final Triple<Long, Integer, List<PeerNode>> parseCmdLine(String appName,
                                                                            String[] args) throws ParseException {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(110);
        Options options = buildCommandlineOptions(new Options());
        CommandLine commandLine = null;
        try {
            commandLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            helpFormatter.printHelp(appName, options, true);
        }
        if (commandLine == null)
            System.exit(-1);

        long id = Long.parseLong(commandLine.getOptionValue("id"));

        AtomicInteger idCnt = new AtomicInteger(0);

        int port = 8888;

        List<PeerNode> peerNodes =
                Arrays.asList(commandLine.getOptionValue("c").split(",")).stream()
                        .map(addr -> {
                            String[] strs = addr.split(":");
                            String ip = strs[0];
                            int p = Integer.parseInt(strs[1]);
                            return new PeerNode(idCnt.incrementAndGet(), ip,
                                    p + NettyPeerServerConfig.PEER_PORT_INCREMENT);
                        }).collect(Collectors.toList());

        if (commandLine.hasOption("p"))
            port = Integer.parseInt(commandLine.getOptionValue("p"));
        return new Triple<>(id, port, peerNodes);
    }

    private static final Options buildCommandlineOptions(Options options) {
        Option port = new Option("p", "port", true, "server port");
        port.setRequired(false);
        options.addOption(new Option("i", "id", true, "raft node id"))
                .addOption(new Option("c", "cluster", true,
                        "raft server address list, eg: 192.168.0.1:9876,192.168.0.2:9876"))
                .addOption(port);
        return options;
    }
}
