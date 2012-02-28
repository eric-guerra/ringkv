package com.ericguerra.ringkv;

import com.ericguerra.ringkv.cluster.FailureDetector;
import com.ericguerra.ringkv.cluster.NettyTransport;
import com.ericguerra.ringkv.node.HybridClock;
import com.ericguerra.ringkv.node.Node;
import com.ericguerra.ringkv.ring.ConsistentHashRing;
import com.ericguerra.ringkv.server.KvServer;
import com.ericguerra.ringkv.storage.DurableStorage;

public final class Main {
    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv(System.getenv());

        var ring = new ConsistentHashRing(config.vnodes(), config.peers());
        var storage = new DurableStorage(config.dataDir().resolve("wal.log"), config.fsync(), 3.0);
        var transport = new NettyTransport(config.rpcTimeoutMs());
        var detector = new FailureDetector(config.self(), config.peers(), transport, config.suspectAfterMs(), System::currentTimeMillis);
        var node = new Node(config.self(), ring, storage, transport, detector, config.quorum(), new HybridClock());
        var server = new KvServer(node);

        server.start(config.self().port());
        detector.start(1000);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            detector.close();
            transport.close();
            storage.close();
        }, "shutdown"));
        Thread.currentThread().join();
    }
}
