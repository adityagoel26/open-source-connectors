package com.boomi.connector.odataclient;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.AtomProxyConfig;
import com.boomi.connector.api.Connector;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleOperationContext;

public class MockOperationContext extends SimpleOperationContext {

	private List<String> selectedFields;
	private String customOperationType;
	public MockOperationContext(AtomConfig config, Connector connector, OperationType opType,
			Map<String, Object> connProps, Map<String, Object> opProps, String objectTypeId,
			Map<ObjectDefinitionRole, String> cookies, List<String> selectedFields) {
		super(config, connector, opType, connProps, opProps, objectTypeId, cookies, selectedFields);
	}

//	@Override
//	public List<String> getSelectedFields() {
//		return selectedFields;
//	}

	@Override
	public String getCustomOperationType() {
		return customOperationType;
	}

	public void setCustomOperationType(String customOperationType) {
		this.customOperationType = customOperationType;
	}
	
	public AtomConfig getConfig()
	{
		AtomConfig atomConfig = new MockAtomConfig();
		return null;
	}
	
	public class MockAtomConfig implements AtomConfig
	{

		@Override
		public String getContainerProperty(String key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getContainerProperty(String key, String defaultValue) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Boolean getBooleanContainerProperty(String key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean getBooleanContainerProperty(String key, boolean defaultValue) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Long getLongContainerProperty(String key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getLongContainerProperty(String key, long defaultValue) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public AtomProxyConfig getProxyConfig() {
			// TODO Auto-generated method stub
			return new MockAtomProxyConfig();
		}

		@Override
		public int getMaxPageSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getMaxNumberObjectTypes() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getMaxObjectTypeCookieLength() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Level getLogLevel() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

	public class MockAtomProxyConfig implements AtomProxyConfig
	{

		@Override
		public boolean isProxyEnabled() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isAuthenticationEnabled() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String getProxyHost() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getProxyPort() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getProxyUser() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getProxyPassword() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getNonProxyHostsString() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Iterable<String> getNonProxyHosts() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
