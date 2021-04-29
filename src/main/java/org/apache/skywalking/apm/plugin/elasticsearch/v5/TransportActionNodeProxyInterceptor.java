/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.plugin.elasticsearch.v5;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceConstructorInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.util.StringUtil;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.XContentHelper;

import static org.apache.skywalking.apm.plugin.elasticsearch.v5.ElasticsearchPluginConfig.Plugin.Elasticsearch.TRACE_DSL;

public class TransportActionNodeProxyInterceptor implements InstanceConstructorInterceptor, InstanceMethodsAroundInterceptor {
    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
        MethodInterceptResult result) throws Throwable {
        ElasticSearchEnhanceInfo enhanceInfo = (ElasticSearchEnhanceInfo) ((EnhancedInstance) objInst.getSkyWalkingDynamicField())
            .getSkyWalkingDynamicField();
        ActionRequest request = (ActionRequest) allArguments[1];
        String opType = allArguments[1].getClass().getSimpleName();
        String operationName = Constants.ELASTICSEARCH_DB_OP_PREFIX + opType;
        AbstractSpan span = ContextManager.createExitSpan(operationName, enhanceInfo.transportAddresses());
        span.setComponent(ComponentsDefine.TRANSPORT_CLIENT);
        Tags.DB_TYPE.set(span, Constants.DB_TYPE);
        Tags.DB_INSTANCE.set(span, enhanceInfo.getClusterName());
        span.tag(Constants.ES_NODE, ((DiscoveryNode) allArguments[0]).getAddress().toString());
        parseRequestInfo(request, span);
        SpanLayer.asDB(span);
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
        Object ret) throws Throwable {
        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().log(t);
    }

    @Override
    public void onConstruct(EnhancedInstance objInst, Object[] allArguments) {
        EnhancedInstance actions = (EnhancedInstance) allArguments[1];
        objInst.setSkyWalkingDynamicField(actions);
    }

    private void parseRequestInfo(ActionRequest request, AbstractSpan span) {
        // search request
        if (request instanceof SearchRequest) {
            parseSearchRequest((SearchRequest) request, span);
            return;
        }
        if (request instanceof MultiSearchRequest) {
            final List<SearchRequest> requests = ((MultiSearchRequest) request).requests();
            span.tag(Constants.ES_INDEX, requests.stream()
                    .map(val -> StringUtil.join(',', val.indices()))
                    .collect(Collectors.joining(",")));
            span.tag(Constants.ES_TYPE, requests.stream()
                    .map(val -> StringUtil.join(',', val.types()))
                    .collect(Collectors.joining(",")));
            if (TRACE_DSL) {
                Tags.DB_STATEMENT.set(span, requests.stream().map(
                        TransportActionNodeProxyInterceptor::getSearchRequestToString).collect(Collectors.joining(",")));
            }
        }
        if (request instanceof BulkRequest) {
            BulkRequest bulkRequest = (BulkRequest) request;
            // todo bulkRequest.requests() may include IndexRequest GetRequest..., now not already support it.
        }
        // get request
        if (request instanceof GetRequest) {
            parseGetRequest((GetRequest) request, span);
            return;
        }
        // index request
        if (request instanceof IndexRequest) {
            parseIndexRequest((IndexRequest) request, span);
            return;
        }
        // update request
        if (request instanceof UpdateRequest) {
            parseUpdateRequest((UpdateRequest) request, span);
            return;
        }
        // delete request
        if (request instanceof DeleteRequest) {
            parseDeleteRequest((DeleteRequest) request, span);
            return;
        }
        // delete index request
        if (request instanceof DeleteIndexRequest) {
            parseDeleteIndexRequest((DeleteIndexRequest) request, span);
            return;
        }
    }

    private void parseSearchRequest(SearchRequest searchRequest, AbstractSpan span) {
        span.tag(Constants.ES_INDEX, StringUtil.join(',', searchRequest.indices()));
        span.tag(Constants.ES_TYPE, StringUtil.join(',', searchRequest.types()));
        if (TRACE_DSL) {
            Tags.DB_STATEMENT.set(span, getSearchRequestToString(searchRequest));
        }
    }

    private void parseGetRequest(GetRequest getRequest, AbstractSpan span) {
        span.tag(Constants.ES_INDEX, getRequest.index());
        span.tag(Constants.ES_TYPE, getRequest.type());
        if (TRACE_DSL) {
            Tags.DB_STATEMENT.set(span, getRequest.toString());
        }
    }

    private void parseIndexRequest(IndexRequest indexRequest, AbstractSpan span) {
        span.tag(Constants.ES_INDEX, indexRequest.index());
        span.tag(Constants.ES_TYPE, indexRequest.type());
        if (TRACE_DSL) {
            Tags.DB_STATEMENT.set(span, indexRequest.toString());
        }
    }

    private void parseUpdateRequest(UpdateRequest updateRequest, AbstractSpan span) {
        span.tag(Constants.ES_INDEX, updateRequest.index());
        span.tag(Constants.ES_TYPE, updateRequest.type());
        if (TRACE_DSL) {
            Tags.DB_STATEMENT.set(span, getUpdateRequestToString(updateRequest));
        }
    }

    private void parseDeleteRequest(DeleteRequest deleteRequest, AbstractSpan span) {
        span.tag(Constants.ES_INDEX, deleteRequest.index());
        span.tag(Constants.ES_TYPE, deleteRequest.type());
        if (TRACE_DSL) {
            Tags.DB_STATEMENT.set(span, deleteRequest.toString());
        }
    }

    private void parseDeleteIndexRequest(DeleteIndexRequest deleteIndexRequest, AbstractSpan span) {
        span.tag(Constants.ES_INDEX, String.join(",", deleteIndexRequest.indices()));
    }

    /**
     * format SearchRequest db.statement value
     *
     * @param searchRequest origin request obj
     * @return value after format
     */
    private static String getSearchRequestToString(SearchRequest searchRequest) {
        try {
            return "SearchRequest{searchType=" + searchRequest.searchType() + ", indices=" + Arrays.toString(searchRequest.indices()) + ", indicesOptions=" + searchRequest.indicesOptions() + ", types=" + Arrays.toString(searchRequest.types()) + ", routing='" + searchRequest.routing() + '\'' + ", preference='" + searchRequest.preference() + '\'' + ", requestCache=" + searchRequest.requestCache() + ", scroll=" + searchRequest.scroll() + ", source=" + XContentHelper.convertToJson(
                    searchRequest.source(), false) + '}';
        } catch (IOException e) {
            return "";
        }
    }

    /**
     * format UpdateRequest db.statement value
     *
     * @param updateRequest origin request obj
     * @return value after format
     */
    private static String getUpdateRequestToString(UpdateRequest updateRequest) {
        StringBuilder res = (new StringBuilder()).append("update {[").append(updateRequest.index()).append("][").append(updateRequest.type()).append("][").append(updateRequest.id()).append("]");
        res.append(", doc_as_upsert[").append(updateRequest.docAsUpsert()).append("]");
        if (updateRequest.doc() != null) {
            res.append(", doc[").append(updateRequest.doc()).append("]");
        }

        if (updateRequest.script() != null) {
            res.append(", script[").append(updateRequest.script()).append("]");
        }

        if (updateRequest.upsertRequest() != null) {
            res.append(", upsert[").append(updateRequest.upsertRequest()).append("]");
        }

        res.append(", scripted_upsert[").append(updateRequest.scriptedUpsert()).append("]");
        res.append(", detect_noop[").append(updateRequest.detectNoop()).append("]");
        if (updateRequest.fields() != null) {
            res.append(", fields[").append(Arrays.toString(updateRequest.fields())).append("]");
        }
        return res.append("}").toString();
    }
}
