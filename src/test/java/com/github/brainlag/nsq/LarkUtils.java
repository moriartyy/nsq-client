package com.github.brainlag.nsq;

import com.github.brainlag.nsq.lookup.DefaultLookup;
import com.github.brainlag.nsq.lookup.Lookup;
import lombok.Getter;
import lombok.Setter;
import mousio.etcd4j.responses.EtcdKeysResponse;
import mtime.lark.util.config.AppConfig;
import mtime.lark.util.config.RemoteLoader;
import mtime.lark.util.convert.StringConverter;
import mtime.lark.util.encode.JsonEncoder;
import mtime.lark.util.etcd.EtcdManager;
import mtime.lark.util.lang.StrKit;
import mtime.lark.util.lang.TypeWrapper;
import mtime.lark.util.lang.UncheckedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.*;

/**
 * @author hongmiao.yu
 */
public class LarkUtils {

    private static final Logger LOGGER = LogManager.getLogger(ProducerTest.class);

    @Getter
    @Setter
    private static class PubNode {
        private String tcp;
        private String http;
        private String note;
    }

    public static Set<ServerAddress> getProduceServersForTopic(String topic) {
        Set<ServerAddress> set = getProduceServersFromConfigCenter(topic);
        if (set.isEmpty()) {
            set = getProduceServicesFromEtcd(topic);
            if (set.isEmpty()) {
                set = getProduceServicesFromEtcd("_default");
            }
        }
        return set;
    }

    public static Set<ServerAddress> getProduceServersFromConfigCenter(String topic) {
        LOGGER.info("getAddressesFromConfigCenter for topic {}", topic);
        Map<String, String> args = new HashMap<>(1);
        args.put("topic", topic);
        RemoteLoader.Result result = RemoteLoader.load("/msg/pub", args);
        if (!result.isSuccess()) {
            LOGGER.error("config > msg: load publish nodes of [{}] from config center failed: {}", topic, result.getError());
            return Collections.emptySet();
        }

        try {
            List<PubNode> nodes = JsonEncoder.DEFAULT.decode(result.getValue(), new TypeWrapper<List<PubNode>>() {
            });
            if (!nodes.isEmpty()) {
                Set<ServerAddress> set = new HashSet<>(nodes.size());
                nodes.forEach(node -> set.add(parseAddress(node.tcp)));
                return set;
            }
        } catch (Exception e) {
            LOGGER.error("config > msg: parse publish nodes of [{}] from config center failed", topic, e);
        }
        return Collections.emptySet();
    }

    public static Set<ServerAddress> getProduceServicesFromEtcd(String topic) {
        LOGGER.info("getAddressesFromEtcd for topic {}", topic);
        try {
            List<EtcdKeysResponse.EtcdNode> nodes = EtcdManager.getChildNodes("/nsq/pub/" + topic);
            if (nodes != null && !nodes.isEmpty()) {
                Set<ServerAddress> set = new HashSet<>(nodes.size());
                nodes.forEach(n -> set.add(parseAddress(EtcdManager.getNodeName(n))));
                return set;
            }
        } catch (Exception e) {
            LOGGER.error("config > msg: load publish nodes of [{}] from etcd failed", topic, e);
        }
        return Collections.emptySet();
    }

    public static ServerAddress parseAddress(String address) {
        String[] array = address.split(":");
        return new ServerAddress(array[0], StringConverter.toInt32(array[1]));
    }

    public static synchronized Lookup getLookup() {
        Lookup lookup = getLookupFromConfigCenter();
        if (lookup == null) {
            lookup = getLookupFromGlobalConfig();
        }
        if (lookup == null) {
            throw new UncheckedException("nsq lookup nodes not found, please check application configurations");
        }
        return lookup;
    }

    public static Lookup getLookupFromGlobalConfig() {
        LOGGER.info("getLookupFromGlobalConfig");
        String value = AppConfig.getDefault().getGlobal().getNsqSubscribeAddress();
        if (StrKit.isBlank(value)) {
            LOGGER.error("config > msg: load subscribe nodes from global.conf failed: no nsq lookup settings");
            return null;
        }

        Lookup nsqLookup = new DefaultLookup();
        String[] array = value.split(",");
        for (String s : array) {
            try {
                URI uri = new URI(s);
                nsqLookup.addLookupAddress(uri.getHost(), uri.getPort());
            } catch (Exception e) {
                LOGGER.error("add lookup address [{}] failed: {}", s, e);
            }
        }
        return nsqLookup;
    }

    public static Lookup getLookupFromConfigCenter() {
        LOGGER.info("getLookupFromConfigCenter");
        RemoteLoader.Result result = RemoteLoader.load("/msg/sub");
        if (!result.isSuccess()) {
            LOGGER.error("config > msg: load subscribe nodes from config center failed: {}", result.getError());
            return null;
        }

        try {
            List<SubNode> nodes = JsonEncoder.DEFAULT.decode(result.getValue(), new TypeWrapper<List<SubNode>>() {
            });

            if (nodes.isEmpty()) {
                LOGGER.error("config > msg: load subscribe nodes from config center failed: no nodes found", result.getError());
                return null;
            }

            Lookup nsqLookup = new DefaultLookup();
            nodes.forEach(n -> addAddress(nsqLookup, n));
            return nsqLookup;
        } catch (Exception e) {
            LOGGER.error("config > msg: parse subscribe nodes from config center failed", e);
        }

        return null;
    }

    private static void addAddress(Lookup lookup, SubNode n) {
        String[] array = n.address.split(":");
        try {
            lookup.addLookupAddress(array[0], Integer.parseInt(array[1]));
        } catch (Exception e) {
            LOGGER.error("add lookup address [{}] failed: {}", n.address, e);
        }
    }

    @Getter
    @Setter
    private static class SubNode {
        private String address;
        private String note;
    }
}
