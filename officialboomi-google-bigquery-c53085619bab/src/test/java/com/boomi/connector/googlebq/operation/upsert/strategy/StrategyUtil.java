// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

final class StrategyUtil {
    private StrategyUtil() {}

    static JsonNode buildErrorJsonNode(String errorMessage) {
        ObjectNode node = JSONUtil.newObjectNode();
        ObjectNode errorNode = JSONUtil.newObjectNode();
        errorNode.put("code", "code");
        errorNode.put("message", errorMessage);
        node.set("error", errorNode);
        return node;
    }
}
