package zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import sun.reflect.generics.tree.Tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Test {
    public static final String zkAddress = "127.0.0.1:2181";

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        client.start();

        watcherCacheTest(client);

    }

    private static void baseApiTest(CuratorFramework client) throws Exception {

        CreateBuilder createBuilder = client.create();
        String userPath = createBuilder.withMode(CreateMode.PERSISTENT)
                .forPath("/user", "test".getBytes());
        Stat stat = client.checkExists().forPath("/user");
        byte[] data = client.getData().forPath("/user");
        String dataStr = new String(data);
        for (int i = 0; i < 5; i++) {
            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/user/child-" + i);
        }
        List<String> children = client.getChildren().forPath("/user");
        System.out.println(children);
        client.delete().deletingChildrenIfNeeded().forPath("/user");
        children = client.getChildren().forPath("/user");
        System.out.println(children);
    }

    private static void asyncApiTest(CuratorFramework client) throws Exception {
        client.getCuratorListenable().addListener(
                new CuratorListener() { // ?????????
                    public void eventReceived(CuratorFramework curatorFramework, CuratorEvent event) throws Exception {
                        switch (event.getType()) {
                            case CREATE:
                                System.out.println("CREATE:" +
                                        event.getPath());
                                break;
                            case DELETE:
                                System.out.println("DELETE:" +
                                        event.getPath());
                                break;
                            case EXISTS:
                                System.out.println("EXISTS:" +
                                        event.getPath());
                                break;
                            case GET_DATA:
                                System.out.println("GET_DATA:" +
                                        event.getPath() + ","
                                        + new String(event.getData()));
                                break;
                            case SET_DATA:
                                System.out.println("SET_DATA:" +
                                        new String(event.getData()));
                                break;
                            case CHILDREN:
                                System.out.println("CHILDREN:" +
                                        event.getPath());
                                break;
                            default:
                        }
                    }
                }
        );
        client.create().withMode(CreateMode.PERSISTENT).inBackground().forPath("/user", "test".getBytes());
        client.checkExists().inBackground().forPath("/user");
        client.setData().inBackground().forPath("/user", "setData-Test".getBytes());
        client.getData().inBackground().forPath("/user");
        for (int i = 0; i < 5; i++) {
            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).inBackground().forPath("/user/child-");
        }
        client.getChildren().inBackground().forPath("/user");
        client.getChildren().inBackground(
                new BackgroundCallback() {
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("in background:" + curatorEvent.getType() + "," + curatorEvent.getPath());
                    }
                }
        ).forPath("/user");
        client.delete().deletingChildrenIfNeeded().inBackground()
                .forPath("/user");
        System.out.println();
    }

    private static void connectionStateListenerTest(CuratorFramework client) throws Exception {
        client.getConnectionStateListenable().addListener(
                new ConnectionStateListener() {
                    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                        switch (connectionState){
                            case CONNECTED: // ??????client??????????????????
                                break;
                            case SUSPENDED: // ??????
                                break;
                            case RECONNECTED: // ??????
                                break;
                            case LOST:  //   session??????
                                break;
                            case READ_ONLY:
                                break;
                        }
                    }
                }
        );
    }


    private static void watcherTest(CuratorFramework client) throws Exception {
        //client.create().withMode(CreateMode.PERSISTENT).forPath("/user", "test".getBytes());
        // ??????????????? ????????????  ???????????????????????????process?????? ??????????????????
        List<String> list = client.getChildren().usingWatcher(new CuratorWatcher() {
            public void process(WatchedEvent watchedEvent) throws Exception {
                System.out.println(watchedEvent.getType() + "," + watchedEvent.getPath());
            }
        }).forPath("/");// ??????/??????????????????
        System.out.println(list);
        System.in.read(); // ???zk???  create /test "test" ??????????????????
    }

    private static void watcherCacheTest(CuratorFramework client) throws Exception {
        NodeCache nodeCache = new NodeCache(client, "/user") ; // ??????????????????  ????????????????????? ??????????????????watcher??????
        nodeCache.start(true);        //???????????????????????????
        System.out.println(new String(nodeCache.getCurrentData().getData()));
        nodeCache.getListenable().addListener(() -> {
            String s = new String(nodeCache.getCurrentData().getData());
            System.out.println(nodeCache.getCurrentData().getPath() + " ---" + s);
        });
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/user", true);
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        List<ChildData> childData = pathChildrenCache.getCurrentData();
        childData.forEach(s -> {
            System.out.println(new String(s.getData()));
        });
        pathChildrenCache.getListenable().addListener((cli, event) -> {
            switch (event.getType()) {
                case INITIALIZED:
                    System.out.println("????????????????????????"); break;
                case CHILD_ADDED:
                    System.out.println("???????????????" + event.getData().getPath() + "," + new String(event.getData().getData()));
                case CHILD_UPDATED:
                    System.out.println("???????????????" + event.getData().getPath() + "," + new String(event.getData().getData()));
                case CHILD_REMOVED:
                    System.out.println("???????????????" + event.getData().getPath());
            }
        });

        TreeCache treeCache = TreeCache.newBuilder(client, "/user").setCacheData(false).build();
        treeCache.getListenable().addListener((cli, event) -> {
            System.out.println(event.getType() + "===" + event.getData().getPath() + "===" + event.getData().getData());
        });
        treeCache.start();
        System.in.read();
    }

    /**
     * zk???????????????????????????  ????????????????????????path???????????????????????????????????????zk???????????????????????????
     */
    private static void curatorXDiscoveryTest(CuratorFramework client) throws Exception {
        Config config = new Config("127.0.0.1",2181, "/user");
        ZookeeperCoordinator zookeeperCoordinator = new ZookeeperCoordinator(config);
        // ??? ?????? 9999 ???????????????zk
        zookeeperCoordinator.registerRemote(new ServerInfo("127.0.0.1", 9999));
    }

    /**
     * ??????????????????????????????
     */
    private static class ZookeeperCoordinator{
        private ServiceDiscovery<ServerInfo> serviceDiscovery;
        private ServiceCache<ServerInfo> serviceCache;
        private CuratorFramework client;
        private String root;
        private InstanceSerializer serializer = new JsonInstanceSerializer<>(ServerInfo.class);
        public ZookeeperCoordinator(Config config) throws Exception {
            this.root = config.getPath();
            client = CuratorFrameworkFactory.newClient(
                    config.getHostPort(), new ExponentialBackoffRetry(50, 3));
            client.start(); // ??????Curator?????????
            client.blockUntilConnected();  // ???????????????????????????????????????
            serviceDiscovery = ServiceDiscoveryBuilder.builder(ServerInfo.class)
                    .client(client)
                    .basePath(root)
                    .watchInstances(true)
                    .serializer(serializer)
                    .build();
            serviceDiscovery.start();
            serviceCache = serviceDiscovery.serviceCacheBuilder()
                    .name(root)
                    .build();
            serviceCache.start();  // ??????????????????????????????
        }
        public void registerRemote(ServerInfo serverInfo) throws Exception {
            // serviceinfo ??????instance
            ServiceInstance<ServerInfo> instance =
                    ServiceInstance.<ServerInfo>builder() // ??????????????????
                    .name(root).id(UUID.randomUUID().toString())
                    .address(serverInfo.getHost())
                    .port(serverInfo.getPort())
                    .payload(serverInfo)
                    .build();
            serviceDiscovery.registerService(instance);
        }
        public List<ServerInfo> queryRemoteAddress() {
            List<ServerInfo> serverInfos = new ArrayList<>();
            List<ServiceInstance<ServerInfo>> instances = serviceCache.getInstances();
            instances.forEach(s -> {
                ServerInfo payload = s.getPayload();
                serverInfos.add(payload);
            });
            return serverInfos;
        }
    }

    private static class Config {
        private String path;
        private String host;
        private int port;

        public Config(String host, int port, String path) {
            this.path = path;
            this.host = host;
            this.port = port;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getHostPort() {
            return host + ":" + port;
        }
    }

    private static class ServerInfo implements Serializable {

        private String host;

        private int port;



        public ServerInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }
}
