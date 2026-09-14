module com.github.f442y.dispersion.routing.api {
    requires static org.jspecify;
    requires transitive com.github.f442y.dispersion.event.api;

    exports com.github.f442y.dispersion.routing;
    exports com.github.f442y.dispersion.routing.endpoint;
    exports com.github.f442y.dispersion.routing.policy;
    exports com.github.f442y.dispersion.routing.backpressure;
    exports com.github.f442y.dispersion.routing.worker;
    exports com.github.f442y.dispersion.routing.transport;
}
