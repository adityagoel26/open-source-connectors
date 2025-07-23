//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.responses;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import com.boomi.util.CollectionUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class modeling the response from Describe Table API for a GET request
 *
 * @author saurav.b.sengupta <saurav.b.sengupta@accenture.com>
 */

public class DescribeTableResponse implements Serializable {

	private Integer total;
	private Data data;
	private static final long serialVersionUID = 1081167279779731988L;

	@JsonCreator
	public DescribeTableResponse(@JsonProperty("total") Integer total, @JsonProperty("data") List<Data> data) {
		super();
		this.total = total;
		this.data = CollectionUtil.getFirst(data);
	}

	public Integer getTotal() {
		return total;
	}

	public Data getData() {
		return data;
	}

	public static class Data implements Serializable {

		private String id;	   
	    private Boolean published;	  
	    private List<Field> fields = new ArrayList<>();	   
	    private String displayName;	   
	    private Permissions permissions;	   
	    private String createdMoment;	   
	    private String updatedMoment;	   
	    private CreatedBy createdBy;	    
	    private String name;	    
	    private Boolean empty;	    
	    private UpdatedBy updatedBy;
		private static final long serialVersionUID = 649349052293228798L;
		
		@JsonCreator
		public Data(@JsonProperty("id") String id,  
				    @JsonProperty("published") Boolean published, 
				    @JsonProperty("fields") List<Field> fields, 
				    @JsonProperty("displayName") String displayName, 
				    @JsonProperty("permissions") Permissions permissions,
				    @JsonProperty("createdMoment") String createdMoment,
				    @JsonProperty("updatedMoment") String updatedMoment, 
				    @JsonProperty("createdBy") CreatedBy createdBy,
				    @JsonProperty("name") String name,
				    @JsonProperty("empty") Boolean empty,
				    @JsonProperty("updatedBy") UpdatedBy updatedBy) {
			super();
			this.id = id;
			this.published = published;
			this.fields = fields;
			this.displayName = displayName;
			this.permissions = permissions;
			this.createdMoment = createdMoment;
			this.updatedMoment = updatedMoment;
			this.createdBy = createdBy;
			this.name = name;
			this.empty = empty;
			this.updatedBy = updatedBy;
		}	
		
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Boolean getPublished() {
			return published;
		}

		public void setPublished(Boolean published) {
			this.published = published;
		}

		public List<Field> getFields() {
			return fields;
		}

		public void setFields(List<Field> fields) {
			this.fields = fields;
		}

		public String getDisplayName() {
			return displayName;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

		public Permissions getPermissions() {
			return permissions;
		}

		public void setPermissions(Permissions permissions) {
			this.permissions = permissions;
		}

		public String getCreatedMoment() {
			return createdMoment;
		}

		public void setCreatedMoment(String createdMoment) {
			this.createdMoment = createdMoment;
		}

		public String getUpdatedMoment() {
			return updatedMoment;
		}

		public void setUpdatedMoment(String updatedMoment) {
			this.updatedMoment = updatedMoment;
		}

		public CreatedBy getCreatedBy() {
			return createdBy;
		}

		public void setCreatedBy(CreatedBy createdBy) {
			this.createdBy = createdBy;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Boolean getEmpty() {
			return empty;
		}

		public void setEmpty(Boolean empty) {
			this.empty = empty;
		}

		public UpdatedBy getUpdatedBy() {
			return updatedBy;
		}

		public void setUpdatedBy(UpdatedBy updatedBy) {
			this.updatedBy = updatedBy;
		}

	}
	
	public static class Field implements Serializable {
		
		
	    private String id;	   
	    private Integer ordinal;	   
	    private String fieldId;	   
	    private Type type;	    
	    private String name;	    
	    private String displayName;	   
	    private Integer scale;	   
	    private Integer precision;	    
	    private String description;
	    private String defaultValue;
	    private String parseFormat;
	    private static final long serialVersionUID = -3808210837517335247L;
	    
	    @JsonCreator
		public Field(@JsonProperty("id") String id, 
				 @JsonProperty("ordinal") Integer ordinal, 
				 @JsonProperty("fieldId") String fieldId, 
				 @JsonProperty("type") Type type, 
				 @JsonProperty("name") String name,
				 @JsonProperty("displayName") String displayName,
				 @JsonProperty("scale") Integer scale, 
				 @JsonProperty("precision") Integer precision, 
				 @JsonProperty("description") String description,
				 @JsonProperty("defaultValue") String defaultValue,
				 @JsonProperty("parseFormat") String parseFormat) {
			super();
			this.id = id;
			this.ordinal = ordinal;
			this.fieldId = fieldId;
			this.type = type;
			this.name = name;
			this.displayName = displayName;
			this.scale = scale;
			this.precision = precision;
			this.description = description;
			this.defaultValue = defaultValue;
			this.parseFormat = parseFormat;
		}

		public String getId() {
			return id;
		}

		public Integer getOrdinal() {
			return ordinal;
		}

		public String getFieldId() {
			return fieldId;
		}

		public Type getType() {
			return type;
		}

		public String getName() {
			return name;
		}

		public String getDisplayName() {
			return displayName;
		}

		public Integer getScale() {
			return scale;
		}

		public Integer getPrecision() {
			return precision;
		}

		public String getDescription() {
			return description;
		}

		public String getDefaultValue() {
			return defaultValue;
		}

		public String getParseFormat() {
			return parseFormat;
		}    

	} 
	
	public static class Type implements Serializable {
		
	    private String descriptor; 
	    private String id;
	    private static final long serialVersionUID = 2600942281813695804L;
		
	    @JsonCreator
	    public Type(@JsonProperty("descriptor") String descriptor, @JsonProperty("id") String id) {
			super();
			this.descriptor = descriptor;
			this.id = id;
		}

		public String getDescriptor() {
			return descriptor;
		}

		public String getId() {
			return id;
		}
	    
	}

	public static class CreatedBy implements Serializable {
		
	    private String id;
	    private String fullName;
	    private static final long serialVersionUID = 1788032558040284046L;
	    
	    @JsonCreator
		public CreatedBy(@JsonProperty("id") String id, @JsonProperty("fullName") String fullName) {
			super();
			this.id = id;
			this.fullName = fullName;
		}

		public String getId() {
			return id;
		}

		public String getFullName() {
			return fullName;
		}
	    
	}

	public static class UpdatedBy implements Serializable {

	    private String id;
	    private String fullName;
	    private static final long serialVersionUID = -5729678066080359976L;
	    
	    @JsonCreator
		public UpdatedBy(@JsonProperty("id") String id, @JsonProperty("fullName") String fullName) {
			super();
			this.id = id;
			this.fullName = fullName;
		}

		public String getId() {
			return id;
		}

		public String getFullName() {
			return fullName;
		}
	    
	}

	public static class Permissions implements Serializable {

		   
		    private Boolean canDelete;		 
		    private Boolean canEdit;		   
		    private Boolean canTruncateTableData;		    
		    private Boolean canShare;		    
		    private Boolean canSelectTableData;		    
		    private Boolean canReplaceTableData;		   
		    private Boolean canView;		   
		    private Boolean canPublish;		   
		    private Boolean canAppendTableData;		    
		    private Boolean canEditDataSourceSecurity;		    
		    private Boolean canDeleteTableData;
		    private static final long serialVersionUID = 7276796916506033259L;
		    
		    @JsonCreator
			public Permissions( @JsonProperty("canDelete") Boolean canDelete, 
					   @JsonProperty("canEdit") Boolean canEdit, 
					   @JsonProperty("canTruncateTableData") Boolean canTruncateTableData, 
					   @JsonProperty("canShare") Boolean canShare,
					   @JsonProperty("canSelectTableData") Boolean canSelectTableData, 
					   @JsonProperty("canReplaceTableData") Boolean canReplaceTableData, 
					   @JsonProperty("canView") Boolean canView, 
					   @JsonProperty("canPublish") Boolean canPublish,
					   @JsonProperty("canAppendTableData") Boolean canAppendTableData, 
					   @JsonProperty("canEditDataSourceSecurity") Boolean canEditDataSourceSecurity, 
					   @JsonProperty("canDeleteTableData") Boolean canDeleteTableData) {
				super();
				this.canDelete = canDelete;
				this.canEdit = canEdit;
				this.canTruncateTableData = canTruncateTableData;
				this.canShare = canShare;
				this.canSelectTableData = canSelectTableData;
				this.canReplaceTableData = canReplaceTableData;
				this.canView = canView;
				this.canPublish = canPublish;
				this.canAppendTableData = canAppendTableData;
				this.canEditDataSourceSecurity = canEditDataSourceSecurity;
				this.canDeleteTableData = canDeleteTableData;
			}

			public Boolean getCanDelete() {
				return canDelete;
			}

			public Boolean getCanEdit() {
				return canEdit;
			}

			public Boolean getCanTruncateTableData() {
				return canTruncateTableData;
			}

			public Boolean getCanShare() {
				return canShare;
			}

			public Boolean getCanSelectTableData() {
				return canSelectTableData;
			}

			public Boolean getCanReplaceTableData() {
				return canReplaceTableData;
			}

			public Boolean getCanView() {
				return canView;
			}

			public Boolean getCanPublish() {
				return canPublish;
			}

			public Boolean getCanAppendTableData() {
				return canAppendTableData;
			}

			public Boolean getCanEditDataSourceSecurity() {
				return canEditDataSourceSecurity;
			}

			public Boolean getCanDeleteTableData() {
				return canDeleteTableData;
			}
		    
		
	}

}
