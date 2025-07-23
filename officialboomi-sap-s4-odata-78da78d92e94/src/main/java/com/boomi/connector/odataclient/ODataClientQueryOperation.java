package com.boomi.connector.odataclient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.util.StreamUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.olingo.odata2.core.commons.Encoder;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Expression;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonParseException;

public class ODataClientQueryOperation extends BaseQueryOperation {

    public enum OperationProperties {MAXDOCUMENTS, PAGESIZE, SERVICEPATH, ALLOWSELECT}

    QueryFilter queryFilter = null;
    long maxDocuments;
    long pageSize;
    String skipTokenPath = null;
    OperationCookie operationCookie;
    ODataParseUtil oDataParseUtil;

    protected ODataClientQueryOperation(ODataClientConnection conn) {
        super(conn);
        oDataParseUtil = new ODataParseUtil();
    }

    /**
     * @param request
     * @param response
     */
    @Override
    protected void executeQuery(QueryRequest request, OperationResponse response) {
        Logger logger = response.getLogger();
        getConnection().setLogger(logger);
        PropertyMap opProps = getContext().getOperationProperties();
        maxDocuments = getMaxDocuments(opProps);
        pageSize = getPageSize(opProps);
        if (getPageSizeQueryParam() != null && pageSize <= 0)
            throw new ConnectorException("Must specify the Page Size to be greater than 0");
        String cookieString = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
        operationCookie = new OperationCookie(cookieString);
        String pageItemPath = ODataConstants.D_RESULTS;

        FilterData input = request.getFilter();
        if (input != null) {
            queryFilter = input.getFilter();
        }
        String path = opProps.getProperty(OperationProperties.SERVICEPATH.name(), "").trim();
        if (StringUtil.isBlank(path))
            throw new ConnectorException("A Service URL Path is required");

        path += getContext().getObjectTypeId();
        //TODO each pathParam must have a filter term in the top level AND....if not throw an error
        try {
            executeQueryResponse(response, logger, opProps, pageItemPath, input, path);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    /**
     * @param response
     * @param logger
     * @param opProps
     * @param pageItemPath
     * @param input
     * @param path
     * @throws IOException
     * @throws GeneralSecurityException
     */
    private void executeQueryResponse(OperationResponse response, Logger logger, PropertyMap opProps, String pageItemPath, FilterData input, String path) throws IOException, GeneralSecurityException {
        CloseableHttpResponse httpResponse = null;
        try {
            String uriParams = "";
            if (opProps.getBooleanProperty(OperationProperties.ALLOWSELECT.name(), false))
                uriParams = appendPath(uriParams, getSelectTermsQueryParam(this.getContext().getSelectedFields()));
            uriParams = appendPath(uriParams, getExpandTermsQueryParameter(this.getContext().getSelectedFields()));
            if (input != null && input.getFilter() != null) {
                if (queryFilter != null) {
                    String filter = getFilterTermsQueryParam(queryFilter.getExpression(), 0);
                    if (filter.length() > 0)
                        uriParams = appendPath(uriParams, "$filter=" + Encoder.encode(filter).replace("+", "%20"));
                }
                uriParams = appendPath(uriParams, getSortTermsQueryParam(queryFilter.getSort()));
            }

            uriParams = appendPath(uriParams, input.getDynamicProperties().get(ODataConstants.EXTRAURIPARAMS));
            if (this.getPageSizeQueryParam() != null)
                uriParams = appendPath(uriParams, this.getPageSizeQueryParam() + "=" + pageSize);
            String fullpath = path + uriParams;
            httpResponse = this.getConnection().doExecute(fullpath, null, ODataConstants.GET, null);
            long numDocuments = 0;
            long numInPage;

            JSONResponseSplitter respSplitter = null;
            do {
                numInPage = 0;
                InputStream is = null;
                try {
                    if (numDocuments > 0)//get the next page from the API
                    {
                        //get the nextpage
                        if (skipTokenPath != null) {
                            fullpath = respSplitter.getSkipToken();
                            fullpath = fullpath.substring(getConnection().getBaseUrl().length()); //Hack off the base URL to get just the path
                        } else {
                            fullpath = path + uriParams + "&" + this.getNextPageQueryParam(numDocuments);
                        }
                        httpResponse = getConnection().doExecute(fullpath, null, ODataConstants.GET, null);
                    }
                    is = httpResponse.getEntity().getContent();
                    int httpStatusCode = httpResponse.getStatusLine().getStatusCode();
                    String httpStatusMessage = httpResponse.getStatusLine().getReasonPhrase();
                    if (is != null && httpStatusCode == 200) {

                        respSplitter = new JSONResponseSplitter(is, pageItemPath, skipTokenPath);
                        for (Payload p : respSplitter) {
                            try (OutputStream tempODataStream = getContext().createTempOutputStream();) {
                                p.writeTo(tempODataStream);

                                try (OutputStream tempOutputStream = getContext().createTempOutputStream()) {
                                    oDataParseUtil.parseODataToBoomi(getContext().tempOutputStreamToInputStream(tempODataStream), tempOutputStream, operationCookie);
                                    numDocuments++;
                                    numInPage++;
                                    PayloadMetadata payloadMetadata = getConnection().setSecurityHeadersToDocumentProperties(getContext());
                                    try (Payload payload = PayloadUtil.toPayload(getContext().tempOutputStreamToInputStream(tempOutputStream), payloadMetadata)) {
                                        response.addPartialResult(input, OperationStatus.SUCCESS, httpStatusCode + "", httpStatusMessage, payload);
                                        if (maxDocuments > 0 && numDocuments >= maxDocuments)
                                            break;
                                    }
                                }

                            } catch (JsonParseException e) {
                                throw new ConnectorException("The SAP API returned malformed JSON. REQUEST: " + fullpath + " ERROR: " + e.getMessage()
                                        + ". Run the query externally to capture the malformed JSON and please contact SAP and provide the full text of this message.");

                            } finally {
                                IOUtil.closeQuietly(p);
                            }
                        }
                    } else {
                        //Sanner is used only for error conditions
                        try (Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {
                            String responseString = scanner.useDelimiter("\\A").next();
                            httpStatusMessage += " " + responseString;
                            is = new ByteArrayInputStream(responseString.getBytes());
                            response.addPartialResult(input, OperationStatus.APPLICATION_ERROR, httpStatusCode + "", httpStatusMessage, ResponseUtil.toPayload(is));
                        }
                    }
                } finally {
                    IOUtil.closeQuietly(is, httpResponse, respSplitter);
                }
                if (maxDocuments > 0 && numDocuments >= maxDocuments)
                    break;
            } while (hasMore(respSplitter, numInPage));
            response.finishPartialResult(input);
        } catch (Exception e) {
            ResponseUtil.addExceptionFailure(response, input, e);
        } finally {
            IOUtil.closeQuietly(httpResponse);
        }
    }

    /**
     * @return
     */
    @Override
    public ODataClientConnection getConnection() {
        return (ODataClientConnection) super.getConnection();
    }

    /**
     * @param respSplitter
     * @param numInPage
     * @return
     */
    private boolean hasMore(JSONResponseSplitter respSplitter, long numInPage) {
        //TODO some may support a hasMore response element but that is redundant for offset pagination which must have a pagesize
        if (skipTokenPath != null)
            return respSplitter.getSkipToken() != null;
        return !(getPageSizeQueryParam() != null && numInPage < pageSize); //If num returned is less than the page size, we are done
    }

    /**
     * @param uriParams
     * @param newParams
     * @return
     */
    private static String appendPath(String uriParams, String newParams) {
        if (newParams != null && newParams.length() > 0) {
            if (uriParams == null)
                uriParams = "";
            if (uriParams.length() > 0)
                uriParams += "&" + newParams;
            else
                uriParams = "?" + newParams;
        }
        return uriParams;
    }

    //TODO remove redundant nav's and watch for selects that are selected without a nav

    /**
     * @param selectedFields
     * @return
     */
    private String getExpandTermsQueryParameter(List<String> selectedFields) {
        List<String> navigations = new ArrayList<String>();
        Map<String, String> optimizedNavigations = new HashMap<String, String>();
        StringBuilder expand = new StringBuilder();
        if (selectedFields != null)
            getNavigationList(selectedFields, navigations);

        //Optimize $expand by leaving the longest navigations
        for (String navigation1 : navigations) {
            String optimumNavigation = navigation1;
            for (String navigation2 : navigations) {
                if (!optimumNavigation.contentEquals(navigation2) && navigation2.startsWith(optimumNavigation))
                    optimumNavigation = navigation2;
            }
            optimizedNavigations.put(optimumNavigation, optimumNavigation);
        }

        for (String navigation : optimizedNavigations.keySet()) {
            if (expand.length() > 0)
                expand.append(",");
            expand.append(navigation);
        }

        if (expand.length() > 0)
            expand.insert(0, "$expand=");
        return expand.toString();
    }

    /**
     * @param selectedFields
     * @param navigations
     */
    private void getNavigationList(List<String> selectedFields, List<String> navigations) {
        for (String select : selectedFields) {
            int lastSlash = select.lastIndexOf("/");
            if (lastSlash > 1) {
                String navigation = select.substring(0, lastSlash);
                String key = "/" + navigation;

                if (operationCookie.isNavigationProperty(key)) {
                    navigations.add(navigation);
                }
            }
        }
    }

    //TODO ideally we would know total fields so we could exclude select if default is all. We could set a `ie for that
    //TODO this could be moved to abstract class if we key on value of PAGINATION_SELECT_URIPARAM not null? Comma delimited fields seems to be the standard when selection is implemented

    /**
     * Build the API URL query parameter to indicate what fields the user selected in the Fields list in the Query Operation UI
     * Defaults to "fields=x,y,z". Note child fields are delimited as parent/child.
     *
     * @param selectedFields the list of fields the user selected for the query operation
     * @return the field URI parameter for the selection
     */
    protected String getSelectTermsQueryParam(List<String> selectedFields) {
        StringBuilder terms = new StringBuilder();

        if (selectedFields != null && selectedFields.size() > 0)// && selected.size()!=totalNumberFields)
        {
            for (String select : selectedFields) {
                if (terms.length() > 0)
                    terms.append(",");
                terms.append(select);
            }
        }
        if (terms.length() > 0)
            terms.insert(0, "$select=");
        return terms.toString();
    }

    /**
     * Build the Filter path parameter from the filters in the Query Operation UI
     * Defaults to setting individual path parameters for each simple expression with each parameter a term in a top level AND expression
     *
     * @return the field URI parameter for the filter
     * @throws IOException
     */
    protected String getFilterTermsQueryParam(Expression baseExpr, int depth) throws IOException {
        StringBuilder queryParameter = new StringBuilder();

        // see if base expression is a single expression or a grouping expression
        if (baseExpr != null) {
            //A single operator, no AND/OR
            if (baseExpr instanceof SimpleExpression) {
                // base expression is a single simple expression
                queryParameter.append(buildSimpleExpression((SimpleExpression) baseExpr));
            } else {
                // handle single level of grouped expressions
                // parse all the simple expressions in the group
                getNestedExpression(depth, queryParameter, (GroupingExpression) baseExpr);
            }
        }
        if (depth > 0)
            queryParameter.append("(" + queryParameter + ")");
        return queryParameter.toString().replace("+", "%20");
    }

    /**
     * @param depth
     * @param queryParameter
     * @param groupExpr
     * @throws IOException
     */
    private void getNestedExpression(int depth, StringBuilder queryParameter, GroupingExpression groupExpr) throws IOException {
        for (Expression nestedExpr : groupExpr.getNestedExpressions()) {
            if (nestedExpr instanceof GroupingExpression) {
                queryParameter.append(getFilterTermsQueryParam(nestedExpr, depth + 1));
            } else {
                String term = (buildSimpleExpression((SimpleExpression) nestedExpr));
                if (term != null && term.length() > 0) {
                    if (queryParameter.length() > 0)
                        queryParameter.append(" " + groupExpr.getOperator().toString().toLowerCase() + " ");
                    queryParameter.append(term);
                }
            }
        }
    }

    /**
     * Override to build a filter expression for the unique grammar of the API.
     * For example create=2021-01-01&createdCompare=lessThan.
     * The default behavior is simple ANDed expressions using the eq operation &email=j@email.com&active=true
     *
     * @param expr the simple expression from which to construct the uri parameter and value
     * @return the URL query parameter and value for the filter compare expression
     * @throws IOException
     */
    protected String buildSimpleExpression(SimpleExpression expr) throws IOException {
        // this is the name of the queried object's property
        String term = "";
        String propName = expr.getProperty();
        String operator = expr.getOperator();

        if (propName == null || propName.length() == 0)
            throw new ConnectorException("Filter field parameter required");
        // we only support 1 argument operations
        if (CollectionUtil.size(expr.getArguments()) != 1)
            throw new IllegalStateException("Unexpected number of arguments for operation " + expr.getOperator() + "; found " +
                    CollectionUtil.size(expr.getArguments()) + ", expected 1");

        // this is the single operation argument
        String parameter = expr.getArguments().get(0);
        if (parameter == null)
            throw new ConnectorException(String.format("Filter parameter is required for field: %s ", propName));
        String type = operationCookie.getEdmType("/" + propName);
        //TODO could the parameter every be a parent/navigation property? If so through an error should be thrown
        parameter = ODataEdmType.boomiValuetoODataPredicate(parameter, type);
        switch (operator) {
            case "startswith":
            case "endswith":
                term = operator + "(" + propName + "," + " " + parameter + ") eq true"; //TODO do we really need eq true?
                break;
            case "substring":
                term = operator + "(" + parameter + "," + propName + ") eq true";
                break;
            default:
                term = propName + " " + operator + " " + parameter + "";
        }

        return term;
    }

    /**
     * Override to build a sort expression for the unique grammar of the API.
     * For example orderby=lastName,firstName
     *
     * @param sortTerms
     * @return the URL query parameter and value for the sort expression
     */
    protected String getSortTermsQueryParam(List<Sort> sortTerms) {
        StringBuilder sortTermsString = new StringBuilder();

        if (sortTerms != null) {
            for (int x = 0; x < sortTerms.size(); x++) {
                Sort sort = (Sort) sortTerms.get(x);
                String sortTerm = sort.getProperty();
                if (sortTerm != null && sortTerm.length() > 0) {
                    if (sortTermsString.length() != 0) {
                        sortTermsString.append(",");
                    }
                    sortTermsString.append(sortTerm);
                    if (sort.getSortOrder() != null)
                        sortTermsString.append(" " + sort.getSortOrder());
                }
            }
        }

        if (sortTermsString.length() != 0) {
            String tempSort = sortTermsString.toString();
            sortTermsString.append("$orderby=" + URLEncoder.encode(tempSort).replace("+", "%20"));
        }
        return sortTermsString.toString();
    }

    /**
     * Override to specify offset pagination path parameters
     * Defaults to offset=<number of documents queried thus far>
     * Note this is not used if getNextPageURLElementPath is set
     *
     * @param numDocuments the number of documents queried thus far
     * @return the path parameter to indicate the next page to query
     */
    protected String getNextPageQueryParam(long numDocuments) {
        return "$skip=" + numDocuments;
    }

    /**
     * Override to specify the name of the parameter that indicates the page size query parameter to specify the number of records per page
     * Defaults to "limit"
     *
     * @return the page size query parameter name
     */
    protected String getPageSizeQueryParam() {
        return ODataConstants.TOP;
    }


    /**
     * @param opProps
     * @return the maximum number of documents set by the user in the query operation page
     */
    public long getMaxDocuments(PropertyMap opProps) {
        return opProps.getLongProperty(OperationProperties.MAXDOCUMENTS.name(), -1L);
    }

    /**
     * @param opProps
     * @return the page size set by the user in the query operation page
     */
    public long getPageSize(PropertyMap opProps) {
        return opProps.getLongProperty(OperationProperties.PAGESIZE.name(), 100L);
    }
}