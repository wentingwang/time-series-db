/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.admin.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.tsdb.action.ReloadBlockAction;
import org.opensearch.tsdb.action.ReloadBlockRequest;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for reloading or downloading blocks into a TSDB index.
 * <p>
 * Supported routes:
 * <ul>
 *   <li>PUT /_tsdb/reload_block/{index} — reload all local blocks</li>
 *   <li>PUT /_tsdb/reload_block/{index}/{block_name} — reload one local block</li>
 *   <li>POST /_tsdb/reload_block — download from URL (terrablob/local path) and load</li>
 * </ul>
 */
public class RestReloadBlockAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestReloadBlockAction.class);

    public static final String NAME = "reload_block_action";
    private static final String ROUTE_PATH = "/_tsdb/reload_block/{index}";
    private static final String ROUTE_PATH_WITH_BLOCK = "/_tsdb/reload_block/{index}/{block_name}";
    private static final String ROUTE_PATH_POST = "/_tsdb/reload_block";

    public RestReloadBlockAction() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, ROUTE_PATH), new Route(PUT, ROUTE_PATH_WITH_BLOCK), new Route(POST, ROUTE_PATH_POST));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ReloadBlockRequest reloadBlockRequest;

        if (request.method() == POST) {
            reloadBlockRequest = parsePostBody(request);
        } else {
            reloadBlockRequest = parsePutParams(request);
        }

        if (reloadBlockRequest == null) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "text/plain", "Index name is required"));
        }

        return channel -> client.execute(ReloadBlockAction.INSTANCE, reloadBlockRequest, new RestToXContentListener<>(channel));
    }

    private ReloadBlockRequest parsePutParams(RestRequest request) {
        String index = request.param("index");
        String blockName = request.param("block_name");
        int shard = request.paramAsInt("shard", 0);

        if (index == null || index.isEmpty()) {
            return null;
        }

        if (blockName != null && !blockName.isEmpty()) {
            logger.debug("Reload block '{}' in index: {} shard: {}", blockName, index, shard);
        } else {
            logger.debug("Reload all local blocks in index: {} shard: {}", index, shard);
        }

        ReloadBlockRequest reloadBlockRequest = new ReloadBlockRequest(index, blockName);
        reloadBlockRequest.setShard(shard);
        return reloadBlockRequest;
    }

    private ReloadBlockRequest parsePostBody(RestRequest request) throws IOException {
        String index = null;
        String url = null;
        int shard = 0;

        try (XContentParser parser = request.contentOrSourceParamParser()) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected JSON object body");
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                switch (fieldName) {
                    case "index":
                        index = parser.text();
                        break;
                    case "url":
                        url = parser.text();
                        break;
                    case "shard":
                        shard = parser.intValue();
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }

        if (index == null || index.isEmpty()) {
            return null;
        }

        logger.debug("POST reload_block: index={}, url={}, shard={}", index, url, shard);

        ReloadBlockRequest reloadBlockRequest = new ReloadBlockRequest(index);
        reloadBlockRequest.setShard(shard);
        if (url != null && !url.isEmpty()) {
            reloadBlockRequest.setUrl(url);
        }
        return reloadBlockRequest;
    }
}
