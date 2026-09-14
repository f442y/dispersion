package com.github.f442y.dispersion.routing;

import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public interface InspectableRouter {

    @NonNull
    Set<String> registeredServices();

    int totalActiveWorkloads();

    int totalRegisteredEndpoints();

    @NonNull
    List<EndpointDescriptor> listEndpointDescriptors(@NonNull String serviceName);

    @NonNull
    Map<String, List<EndpointDescriptor>> allEndpoints();

    record EndpointDescriptor(
            @NonNull String endpointId,
            @NonNull String serviceName,
            @NonNull EndpointType endpointType,
            @NonNull EndpointStatus status,
            @NonNull EndpointTags tags,
            int activeWorkloads,
            int maxConcurrency
    ) {
        public EndpointDescriptor {
            Objects.requireNonNull(endpointId, "endpointId must not be null");
            Objects.requireNonNull(serviceName, "serviceName must not be null");
            Objects.requireNonNull(endpointType, "endpointType must not be null");
            Objects.requireNonNull(status, "status must not be null");
            Objects.requireNonNull(tags, "tags must not be null");
        }
    }
}
