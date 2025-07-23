// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.xml;

class Pair<K, V> {

    private final K _key;
    private final V _value;

    public Pair(K key, V value) {
        _key = key;
        _value = value;
    }

    public K getKey() {
        return _key;
    }

    public V getValue() {
        return _value;
    }
}
