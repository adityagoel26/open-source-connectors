// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.jmssdk.util.Utils;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.NamingException;

import java.io.Closeable;

/**
 * The initial context for WebLogic Adapters is tied to the thread in which it is created and cannot be shared without
 * affecting user scopes.
 *
 * @see <a href="http://docs.oracle.com/cd/E13222_01/wls/docs81/jndi/jndi.html#471919">WebLogic JNDI</a>
 */
class JndiContext implements Closeable {

    private Context _context;
    private ConnectionFactory _connectionFactory;

    private final GenericJndiBaseAdapter _adapter;

    JndiContext(GenericJndiBaseAdapter adapter) {
        _adapter = adapter;
    }

    Object lookup(String name) throws NamingException {
        return getInitialContext().lookup(name);
    }

    ConnectionFactory getConnectionFactory() throws NamingException {
        if (_connectionFactory == null) {
            _connectionFactory = (ConnectionFactory) lookup(_adapter.getSettings().getJndiLookupFactory());
        }
        return _connectionFactory;
    }

    private Context getInitialContext() {
        if (_context == null) {
            _context = _adapter.createInitialContext();
        }
        return _context;
    }

    @Override
    public void close() {
        Utils.closeQuietly(_context);
    }
}