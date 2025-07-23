//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.Expression;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.GroupingOperator;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.sftp.common.FileMetadata;
import com.boomi.connector.sftp.common.UnixPathsHandler;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * The Class FileQueryFilter.
 *
 * @author Omesh Deoli
 * 
 *
 */
public class FileQueryFilter implements DirectoryStream.Filter<LsEntry> {

	/** The Constant GET_FILENAME. */
	private static final CollectionUtil.Function<LsEntry, String> GET_FILENAME = new CollectionUtil.Function<LsEntry, String>() {

		public String apply(LsEntry fileMetadata) {

			return fileMetadata.getFilename();
		}
	};

	/** The Constant GET_FILESIZE. */
	private static final CollectionUtil.Function<LsEntry, Long> GET_FILESIZE = new CollectionUtil.Function<LsEntry, Long>() {

		public Long apply(LsEntry fileMetadata) {
			return fileMetadata.getAttrs().getSize();
		}
	};

	/** The Constant IS_DIRECTORY. */
	private static final CollectionUtil.Function<LsEntry, Boolean> IS_DIRECTORY = new CollectionUtil.Function<LsEntry, Boolean>() {

		public Boolean apply(LsEntry fileMetadata) {
			return fileMetadata.getAttrs().isDir();
		}
	};

	/** The Constant GET_MODIFIED_DATE. */
	private static final CollectionUtil.Function<LsEntry, Date> GET_MODIFIED_DATE = new CollectionUtil.Function<LsEntry, Date>() {

		public Date apply(LsEntry fileMetadata) {
			try {
				return FileMetadata.parseDate(
						FileMetadata.formatDate(FileMetadata.parseDate(fileMetadata.getAttrs().getMTime() * 1000L)));
			} catch (ParseException e) {
				throw new IllegalArgumentException(SFTPConstants.ERROR_PARSING_DATE_FROM_FILESYSTEM, e);
			}
		}
	};

	/** The Constant DEFAULT_FILTER. */
	private static final CollectionUtil.Filter<LsEntry> DEFAULT_FILTER = CollectionUtil.acceptAllFilter();

	/** The sub filter. */
	private final CollectionUtil.Filter<LsEntry> subFilter;

	/** The logger. */
	private final Logger logger;

	/** The directory. */
	private final Path directory;

	/** The input. */
	private FilterData input;

	/** The dir full path. */
	private String dirFullPath;
	

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public FilterData getInput() {
		return input;
	}

	/**
	 * Instantiates a new file query filter.
	 *
	 * @param directory   the directory
	 * @param input       the input
	 * @param dirFullPath the dir full path
	 */
	public FileQueryFilter(File directory, FilterData input, String dirFullPath) {

		this.directory = directory.toPath();
		this.input = input;
		this.logger = input.getLogger();
		Expression rootExpr = input.getFilter().getExpression();
		this.subFilter = rootExpr == null ? DEFAULT_FILTER : this.buildFilter(rootExpr);
		this.dirFullPath = dirFullPath;

	}

	/**
	 * Accept.
	 *
	 * @param meta the meta
	 * @return true, if successful
	 */
	@Override
	public boolean accept(LsEntry meta) {
		return this.subFilter.accept(meta);
	}

	/**
	 * Builds the filter.
	 *
	 * @param expression the expression
	 * @return the collection util. filter
	 */
	private CollectionUtil.Filter<LsEntry> buildFilter(Expression expression) {
		if (expression instanceof GroupingExpression) {
			return this.group((GroupingExpression) expression);
		}
		if (expression instanceof SimpleExpression) {
			return this.simple((SimpleExpression) expression);
		}
		throw new IllegalArgumentException(SFTPConstants.UNKNOWN_EXPRESSION + expression.getClass().getCanonicalName());
	}

	/**
	 * Group.
	 *
	 * @param expression the expression
	 * @return the collection util. filter
	 */
	private CollectionUtil.Filter<LsEntry> group(GroupingExpression expression) {
		LinkedList<CollectionUtil.Filter<LsEntry>> filters = new LinkedList<>();
		for (Expression e : expression.getNestedExpressions()) {
			filters.add(this.buildFilter(e));
		}
		return new FilterGroup(filters, expression.getOperator());
	}

	/**
	 * Simple.
	 *
	 * @param expression the expression
	 * @return the collection util. filter
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private CollectionUtil.Filter<LsEntry> simple(SimpleExpression expression) {

		if (expression.getArguments().size() != 1) {
			throw new IllegalArgumentException(
					MessageFormat.format(SFTPConstants.EXACTLY_ONE_ARGUEMENT_REQUIRED, expression.getArguments().size()));

		}
		if (SFTPConstants.OP_REGEX.equals(expression.getOperator()) || SFTPConstants.OP_WILDCARD.equals(expression.getOperator())) {
			if (!SFTPConstants.PROPERTY_FILENAME.equals(expression.getProperty())) {
				throw new UnsupportedOperationException(SFTPConstants.PATTERN_UNSUPPORTED_FOR_FILENAMES);
			}
			return new PatternFilter(expression.getOperator(), expression.getArguments().get(0));
		}
		CompFilter compFilter = null;
		switch (expression.getProperty()) {
		case SFTPConstants.PROPERTY_FILENAME:
			compFilter = new CompFilter(GET_FILENAME, Operation.valueOf(expression.getOperator()), (Comparable) (expression.getArguments().get(0)));
			break;
		case SFTPConstants.FILESIZE: 
			compFilter = new CompFilter(GET_FILESIZE, Operation.valueOf(expression.getOperator()), Long.valueOf(Long.parseLong(expression.getArguments().get(0))));
			break;
		case SFTPConstants.IS_DIRECTORY: 
			compFilter = new CompFilter(IS_DIRECTORY, Operation.valueOf(expression.getOperator()), Boolean.valueOf(FileQueryFilter.parseBoolean(expression.getArguments().get(0))));
			break;
		case SFTPConstants.MODIFIED_DATE:
			compFilter = new CompFilter(GET_MODIFIED_DATE, Operation.valueOf(expression.getOperator()), FileQueryFilter.parseDate(expression.getArguments().get(0)));
			break;
		default: 
			throw new IllegalArgumentException(SFTPConstants.UNKNOWN_PROPERTY + expression.getProperty());
		}
		return compFilter;
	}

	/**
	 * Parses the boolean.
	 *
	 * @param input the input
	 * @return true, if successful
	 */
	private static boolean parseBoolean(String input) {
		if ("true".equalsIgnoreCase(input)) {
			return true;
		}
		if ("false".equalsIgnoreCase(input)) {
			return false;
		}
		throw new IllegalArgumentException(SFTPConstants.QUOTE + input + SFTPConstants.INVALID_BOOLEAN_VALUE);
	}

	/**
	 * Parses the date.
	 *
	 * @param date the date
	 * @return the date
	 */
	private static Date parseDate(String date) {
		try {
			return FileMetadata.parseDate(date);
		} catch (ParseException e) {
			throw new IllegalArgumentException(SFTPConstants.UNABLE_TO_PARSE_DATE + date + SFTPConstants.QUOTE, e);
		}
	}

	/**
	 * The Class PatternFilter.
	 */
	private class PatternFilter implements CollectionUtil.Filter<LsEntry> {

		/** The op. */
		private final String op;

		/** The matcher. */
		private final PathMatcher matcher;

		/** The pattern. */
		private final String pattern;

		/**
		 * Instantiates a new pattern filter.
		 *
		 * @param fs      the fs
		 * @param op      the op
		 * @param pattern the pattern
		 */
		PatternFilter(FileSystem fs, String op, String pattern) {
			String mode = SFTPConstants.OP_REGEX.equals(op) ? SFTPConstants.REGEX : SFTPConstants.GLOB;
			this.matcher = fs.getPathMatcher(mode + ':' + pattern);
			this.pattern = pattern;
			this.op = op;
		}

		/**
		 * Instantiates a new pattern filter.
		 *
		 * @param op      the op
		 * @param pattern the pattern
		 */
		PatternFilter(String op, String pattern) {
			this(FileSystems.getDefault(), op, pattern);
		}

		/**
		 * Accept.
		 *
		 * @param meta the meta
		 * @return true, if successful
		 */
		public boolean accept(LsEntry meta) {
			String fullFilePath = new UnixPathsHandler().joinPaths(dirFullPath, meta.getFilename());
			File fullFilePathObj = new File(fullFilePath);
			Path path = FileQueryFilter.this.directory.relativize(fullFilePathObj.toPath());
			boolean res = this.matcher.matches(path);
			if (FileQueryFilter.this.logger.isLoggable(Level.FINE)) {
				LogUtil.fine(FileQueryFilter.this.logger, SFTPConstants.MATCHING_WITH_PATTERN,path.toString(), this.pattern, this.op, res);
			}
			return res;
		}
	}

	/**
	 * The Class CompFilter.
	 *
	 * @param <E> the element type
	 */
	private class CompFilter<E extends Comparable<E>> implements CollectionUtil.Filter<LsEntry> {

		/** The left getter. */
		private final CollectionUtil.Function<LsEntry, E> leftGetter;

		/** The op. */
		private final Operation op;

		/** The right side. */
		private final E rightSide;

		/**
		 * Instantiates a new comp filter.
		 *
		 * @param leftGetter the left getter
		 * @param op         the op
		 * @param rightSide  the right side
		 */
		private CompFilter(CollectionUtil.Function<LsEntry, E> leftGetter, Operation op, E rightSide) {
			this.leftGetter = leftGetter;
			this.op = op;
			this.rightSide = rightSide;
		}

		/**
		 * Accept.
		 *
		 * @param fileMetadata the file metadata
		 * @return true, if successful
		 */
		public boolean accept(LsEntry fileMetadata) {
			E left = this.leftGetter.apply(fileMetadata);
			boolean res = this.op.accept(left, this.rightSide);
			if (logger.isLoggable(Level.FINE)) {
				LogUtil.fine(logger, SFTPConstants.COMPARISON_YIELDS,left, this.op, this.rightSide, res);
			}
			return res;
		}
	}

	/**
	 * The Class FilterGroup.
	 */
	private static class FilterGroup implements CollectionUtil.Filter<LsEntry> {

		/** The filters. */
		private final List<CollectionUtil.Filter<LsEntry>> filters;

		/** The initial val. */
		private final boolean initialVal;

		/**
		 * Instantiates a new filter group.
		 *
		 * @param filters the filters
		 * @param op      the op
		 */
		protected FilterGroup(List<CollectionUtil.Filter<LsEntry>> filters, GroupingOperator op) {
			this.filters = filters;
			if (op == GroupingOperator.AND) {
				this.initialVal = true;
			} else if (op == GroupingOperator.OR) {
				this.initialVal = false;
			} else {
				throw new IllegalArgumentException(SFTPConstants.UNKNOWN_GROUPING_OPERATOR + op);
			}
		}

		/**
		 * Accept.
		 *
		 * @param fileMetadata the file metadata
		 * @return true, if successful
		 */
		public boolean accept(LsEntry fileMetadata) {
			boolean res;
			res = this.initialVal;
			Iterator<CollectionUtil.Filter<LsEntry>> is = this.filters.iterator();
			while (is.hasNext()) {
				res = is.next().accept(fileMetadata);
				if(res != this.initialVal){
					break;
				}
			}
			return res;
		}
	}

	/**
	 * The Enum Operation.
	 */
	private enum Operation {

		/** The equals. */
		EQUALS(new CollectionUtil.Filter<Integer>() {

			public boolean accept(Integer integer) {
				return integer == 0;
			}
		}),
		/** The not equals. */
		NOT_EQUALS(new CollectionUtil.Filter<Integer>() {

			public boolean accept(Integer integer) {
				return integer != 0;
			}
		}),
		/** The less than. */
		LESS_THAN(new CollectionUtil.Filter<Integer>() {

			public boolean accept(Integer integer) {
				return integer < 0;
			}
		}),
		/** The greater than. */
		GREATER_THAN(new CollectionUtil.Filter<Integer>() {

			public boolean accept(Integer integer) {
				return integer > 0;
			}
		});

		/** The filter. */
		private final CollectionUtil.Filter<Integer> filter;

		/**
		 * Instantiates a new operation.
		 *
		 * @param filter the filter
		 */
		private Operation(CollectionUtil.Filter<Integer> filter) {
			this.filter = filter;
		}

		/**
		 * Accept.
		 *
		 * @param <E>   the element type
		 * @param left  the left
		 * @param right the right
		 * @return true, if successful
		 */
		public <E extends Comparable<E>> boolean accept(E left, E right) {
			return this.filter.accept(left.compareTo(right));
		}

	}

}
