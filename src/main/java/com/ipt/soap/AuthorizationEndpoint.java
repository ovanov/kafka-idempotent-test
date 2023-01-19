package com.ipt.soap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;

@Endpoint
public class AuthorizationEndpoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationEndpoint.class);
    private static final String NAMESPACE_URI = "soap.ipt.ch";

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "postAuthorization")
    public void postAuthorization(@RequestPayload soap.ch.ipt.com.schemas.PostAuthorization request) {

        LOGGER.info("Request received: {}", request);
    }
}