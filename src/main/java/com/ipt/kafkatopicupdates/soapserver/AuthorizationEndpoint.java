package com.ipt.kafkatopicupdates.soapserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import soap.ch.ipt.GetAuthorizationRequest;

@Endpoint
public class AuthorizationEndpoint {
    @Value("${topics.authorizations-with-duplicates}")
    private String sourceTopic;
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationEndpoint.class);
    private static final String NAMESPACE_URI = "ipt.ch.soap";

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "getAuthorizationRequest")
    public void authorizationRequest(@RequestPayload GetAuthorizationRequest request) {

        LOGGER.info("Request received: {}", request);
    }
}