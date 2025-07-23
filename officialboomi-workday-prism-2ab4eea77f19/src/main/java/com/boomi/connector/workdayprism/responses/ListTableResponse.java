//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.responses;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class modeling the response from List Tables API for a GET request
 *
 * @author saurav.b.sengupta <saurav.b.sengupta@accenture.com>
 */
public class ListTableResponse implements Serializable {

	
	private Integer total;
	
	private List<Data> data = new ArrayList<>();

	private static final  long serialVersionUID = -6459516998026167561L;

	@JsonCreator
	public ListTableResponse(@JsonProperty("total") Integer total, @JsonProperty("data") List<Data> data) {
		super();
		this.total = total;
		this.data = data;
	}

	public Integer getTotal() {
		return total;
	}

	public List<Data> getData() {
		return data;
	}

	public static class Data implements Serializable {

		private static final long serialVersionUID = -61L;

		
		private String id;
		
		private Boolean empty;
		
		private String name;
	
		private String displayName;
	
		private Boolean published;
	
		private String updatedMoment;
		
		private CreatedBy createdBy;
		
		private Permissions permissions;
		
		private UpdatedBy updatedBy;
		
		private String createdMoment;
		
		private Stats stats;
		
		private String description;

		@JsonCreator
		public Data(@JsonProperty("id") String id, @JsonProperty("empty") Boolean empty, @JsonProperty("name") String name, 
				@JsonProperty("displayName") String displayName, 
				@JsonProperty("published") Boolean published, 
				@JsonProperty("updatedMoment") String updatedMoment,
				@JsonProperty("createdBy") CreatedBy createdBy, 
				@JsonProperty("permissions") Permissions permissions, 
				@JsonProperty("updatedBy") UpdatedBy updatedBy, 
				@JsonProperty("createdMoment") String createdMoment, 
				@JsonProperty("stats") Stats stats,
				@JsonProperty("description") String description) {
			super();
			this.id = id;
			this.empty = empty;
			this.name = name;
			this.displayName = displayName;
			this.published = published;
			this.updatedMoment = updatedMoment;
			this.createdBy = createdBy;
			this.permissions = permissions;
			this.updatedBy = updatedBy;
			this.createdMoment = createdMoment;
			this.stats = stats;
			this.description = description;
		}

		public String getId() {
			return id;
		}

		public Boolean getEmpty() {
			return empty;
		}

		public String getName() {
			return name;
		}

		public String getDisplayName() {
			return displayName;
		}

		public Boolean getPublished() {
			return published;
		}

		public String getUpdatedMoment() {
			return updatedMoment;
		}

		public CreatedBy getCreatedBy() {
			return createdBy;
		}

		public Permissions getPermissions() {
			return permissions;
		}

		public UpdatedBy getUpdatedBy() {
			return updatedBy;
		}

		public String getCreatedMoment() {
			return createdMoment;
		}

		public Stats getStats() {
			return stats;
		}

		public String getDescription() {
			return description;
		}

	}

	public static class Permissions implements Serializable {

		@JsonProperty("canDelete")
		private Boolean canDelete;
		@JsonProperty("canReplaceTableData")
		private Boolean canReplaceTableData;
		@JsonProperty("canDeleteTableData")
		private Boolean canDeleteTableData;
		@JsonProperty("canEdit")
		private Boolean canEdit;
		@JsonProperty("canEditDataSourceSecurity")
		private Boolean canEditDataSourceSecurity;
		@JsonProperty("canShare")
		private Boolean canShare;
		@JsonProperty("canView")
		private Boolean canView;
		@JsonProperty("canAppendTableData")
		private Boolean canAppendTableData;
		@JsonProperty("canSelectTableData")
		private Boolean canSelectTableData;
		@JsonProperty("canTruncateTableData")
		private Boolean canTruncateTableData;
		@JsonProperty("canPublish")
		private Boolean canPublish;
		private static final long serialVersionUID = 7276796916506033259L;

		@JsonCreator
		public Permissions(@JsonProperty("canDelete") Boolean canDelete, 
				@JsonProperty("canReplaceTableData") Boolean canReplaceTableData, 
				@JsonProperty("canDeleteTableData") Boolean canDeleteTableData, 
				@JsonProperty("canEdit") Boolean canEdit,
				@JsonProperty("canEditDataSourceSecurity") Boolean canEditDataSourceSecurity, 
				@JsonProperty("canShare") Boolean canShare, 
				@JsonProperty("canView") Boolean canView, 
				@JsonProperty("canAppendTableData") Boolean canAppendTableData,
				@JsonProperty("canSelectTableData") Boolean canSelectTableData, 
				@JsonProperty("canTruncateTableData") Boolean canTruncateTableData, 
				@JsonProperty("canPublish") Boolean canPublish) {
			super();
			this.canDelete = canDelete;
			this.canReplaceTableData = canReplaceTableData;
			this.canDeleteTableData = canDeleteTableData;
			this.canEdit = canEdit;
			this.canEditDataSourceSecurity = canEditDataSourceSecurity;
			this.canShare = canShare;
			this.canView = canView;
			this.canAppendTableData = canAppendTableData;
			this.canSelectTableData = canSelectTableData;
			this.canTruncateTableData = canTruncateTableData;
			this.canPublish = canPublish;
		}

		public Boolean getCanDelete() {
			return canDelete;
		}

		public Boolean getCanReplaceTableData() {
			return canReplaceTableData;
		}

		public Boolean getCanDeleteTableData() {
			return canDeleteTableData;
		}

		public Boolean getCanEdit() {
			return canEdit;
		}

		public Boolean getCanEditDataSourceSecurity() {
			return canEditDataSourceSecurity;
		}

		public Boolean getCanShare() {
			return canShare;
		}

		public Boolean getCanView() {
			return canView;
		}

		public Boolean getCanAppendTableData() {
			return canAppendTableData;
		}

		public Boolean getCanSelectTableData() {
			return canSelectTableData;
		}

		public Boolean getCanTruncateTableData() {
			return canTruncateTableData;
		}

		public Boolean getCanPublish() {
			return canPublish;
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

	public static class Stats implements Serializable {

		
	    private Integer rows;
	    
	    private String size;
	    private static final long serialVersionUID = -7883047072177909983L;
		
	    @JsonCreator
	    public Stats(@JsonProperty("rows") Integer rows, @JsonProperty("size") String size) {
			super();
			this.rows = rows;
			this.size = size;
		}
		public Integer getRows() {
			return rows;
		}
		public String getSize() {
			return size;
		}
	    
	    
		
	}

}
