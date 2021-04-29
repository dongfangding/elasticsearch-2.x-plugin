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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.util.StringUtil;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import static org.apache.skywalking.apm.plugin.elasticsearch.v5.Constants.ES_TOOK_MILLIS;
import static org.apache.skywalking.apm.plugin.elasticsearch.v5.Constants.ES_TOTAL_HITS;
import static org.apache.skywalking.apm.plugin.elasticsearch.v5.ElasticsearchPluginConfig.Plugin.Elasticsearch.ELASTICSEARCH_DSL_LENGTH_THRESHOLD;
import static org.apache.skywalking.apm.plugin.elasticsearch.v5.ElasticsearchPluginConfig.Plugin.Elasticsearch.TRACE_DSL;

public class PlainListenableActionFutureInterceptor implements InstanceMethodsAroundInterceptor {

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
            MethodInterceptResult result) throws Throwable {
        AbstractSpan span = ContextManager.createLocalSpan(
                Constants.ELASTICSEARCH_DB_OP_PREFIX + Constants.BASE_FUTURE_METHOD);
        span.setComponent(ComponentsDefine.TRANSPORT_CLIENT);
        Tags.DB_TYPE.set(span, Constants.DB_TYPE);
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
            Object ret) throws Throwable {
        AbstractSpan span = ContextManager.activeSpan();
        ActionResponse actionResponse = (ActionResponse) ret;
        parseResponseInfo(actionResponse, span);
        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
            Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().log(t);
    }

    private void parseResponseInfo(ActionResponse response, AbstractSpan span) {
        // search response
        if (response instanceof SearchResponse) {
            parseSearchResponse((SearchResponse) response, span);
            return;
        }
        // bulk response
        if (response instanceof BulkResponse) {
            parseBulkResponse((BulkResponse) response, span);
            return;
        }
        // get response
        if (response instanceof GetResponse) {
            parseGetResponse((GetResponse) response, span);
            return;
        }
        // index response
        if (response instanceof IndexResponse) {
            parseIndexResponse((IndexResponse) response, span);
            return;
        }
        // update response
        if (response instanceof UpdateResponse) {
            parseUpdateResponse((UpdateResponse) response, span);
            return;
        }
        // delete response
        if (response instanceof DeleteResponse) {
            parseDeleteResponse((DeleteResponse) response, span);
            return;
        }
        // MultiSearchResponse
        if (response instanceof MultiSearchResponse) {
            parseMultiSearchResponse((MultiSearchResponse) response, span);
            return;
        }
    }

    private void parseSearchResponse(SearchResponse searchResponse, AbstractSpan span) {
        span.tag(Constants.ES_TOOK_MILLIS, Long.toString(searchResponse.getTook().getMillis()));
        span.tag(ES_TOTAL_HITS, Long.toString(searchResponse.getHits().getTotalHits()));
        if (TRACE_DSL) {
            String tagValue = searchResponse.toString();
            tagValue = ELASTICSEARCH_DSL_LENGTH_THRESHOLD > 0 ? StringUtil.cut(
                    tagValue, ELASTICSEARCH_DSL_LENGTH_THRESHOLD) : tagValue;
            Tags.DB_STATEMENT.set(span, tagValue);
        }
    }

    private void parseMultiSearchResponse(MultiSearchResponse searchResponse, AbstractSpan span) {
        MultiSearchResponse response = (MultiSearchResponse) searchResponse;
        final MultiSearchResponse.Item[] responses = response.getResponses();
        final List<String> hitsList = Arrays.stream(responses)
                .map(val -> String.valueOf(val.getResponse().getTookInMillis()))
                .collect(Collectors.toList());
        final List<String> tookList = Arrays.stream(responses)
                .map(val -> String.valueOf(val.getResponse().getHits().getTotalHits()))
                .collect(Collectors.toList());
        span.tag(ES_TOOK_MILLIS, String.join(",", tookList));
        span.tag(ES_TOTAL_HITS, String.join(",", hitsList));
    }

    private void parseBulkResponse(BulkResponse bulkResponse, AbstractSpan span) {
        span.tag(Constants.ES_TOOK_MILLIS, Long.toString(bulkResponse.getTook().getMillis()));
        //        span.tag(Constants.ES_INGEST_TOOK_MILLIS, Long.toString(bulkResponse.getIngestTookInMillis()));
        if (TRACE_DSL) {
            String tagValue = bulkResponse.toString();
            tagValue = ELASTICSEARCH_DSL_LENGTH_THRESHOLD > 0 ? StringUtil.cut(
                    tagValue, ELASTICSEARCH_DSL_LENGTH_THRESHOLD) : tagValue;
            Tags.DB_STATEMENT.set(span, tagValue);
        }
    }

    private void parseGetResponse(GetResponse getResponse, AbstractSpan span) {
        if (TRACE_DSL) {
            String tagValue = "IndexResponse[" + "index=" + getResponse.getIndex() + ",type=" + getResponse.getType()
                    + ",id=" + getResponse.getId() + ",version=" + getResponse.getVersion() + ",source="
                    + getResponse.getSourceAsString() + "]";
            Tags.DB_STATEMENT.set(span, tagValue);
        }
    }

    private void parseIndexResponse(IndexResponse indexResponse, AbstractSpan span) {
        if (TRACE_DSL) {
            String tagValue = getIndexResponse(indexResponse);
            tagValue = ELASTICSEARCH_DSL_LENGTH_THRESHOLD > 0 ? StringUtil.cut(
                    tagValue, ELASTICSEARCH_DSL_LENGTH_THRESHOLD) : tagValue;
            Tags.DB_STATEMENT.set(span, tagValue);
        }
    }

    private void parseUpdateResponse(UpdateResponse updateResponse, AbstractSpan span) {
        if (TRACE_DSL) {
            String tagValue = getUpdaterResponse(updateResponse);
            tagValue = ELASTICSEARCH_DSL_LENGTH_THRESHOLD > 0 ? StringUtil.cut(
                    tagValue, ELASTICSEARCH_DSL_LENGTH_THRESHOLD) : tagValue;
            Tags.DB_STATEMENT.set(span, tagValue);
        }
    }

    private void parseDeleteResponse(DeleteResponse deleteResponse, AbstractSpan span) {
        if (TRACE_DSL) {
            String tagValue = getDeleteResponse(deleteResponse);
            tagValue = ELASTICSEARCH_DSL_LENGTH_THRESHOLD > 0 ? StringUtil.cut(
                    tagValue, ELASTICSEARCH_DSL_LENGTH_THRESHOLD) : tagValue;
            Tags.DB_STATEMENT.set(span, tagValue);
        }
    }


    /**
     * format IndexResponse value
     *
     * @param indexResponse origin IndexResponse
     * @return value after format
     */
    public static String getIndexResponse(IndexResponse indexResponse) {
        return "IndexResponse[" + "index=" + indexResponse.getIndex() + ",type=" + indexResponse.getType() + ",id="
                + indexResponse.getId() + ",version=" + indexResponse.getVersion() + ",created="
                + indexResponse.isCreated() + "]";
    }

    /**
     * format UpdateResponse value
     *
     * @param updateResponse origin UpdateResponse
     * @return value after format
     */
    public static String getUpdaterResponse(UpdateResponse updateResponse) {
        StringBuilder builder = new StringBuilder();
        builder.append("UpdateResponse[");
        builder.append("index=").append(updateResponse.getIndex());
        builder.append(",type=").append(updateResponse.getType());
        builder.append(",id=").append(updateResponse.getId());
        builder.append(",version=").append(updateResponse.getVersion());
        builder.append(",created=").append(updateResponse.isCreated());
        try {
            builder.append(",result=").append(updateResponse.getGetResult().toXContent(JsonXContent.contentBuilder(), null).string());
        } catch (Exception ignored) {
        }
        builder.append(",shards-successful=").append(updateResponse.getShardInfo().getSuccessful());
        return builder.append("]").toString();
    }


    /**
     * format DeleteResponse value
     *
     * @param deleteResponse origin DeleteResponse
     * @return value after format
     */
    public String getDeleteResponse(DeleteResponse deleteResponse) {
        return "DeleteResponse[" + "index=" + deleteResponse.getIndex() + ",type=" + deleteResponse.getType() + ",id="
                + deleteResponse.getId() + ",version=" + deleteResponse.getVersion() + ",found="
                + deleteResponse.isFound() + "]";
    }
}
