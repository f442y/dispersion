module com.github.f442y.dispersion.routing.core {
    requires static org.jspecify;
    requires org.slf4j;
    requires transitive com.github.f442y.dispersion.routing.api;
    requires transitive com.github.f442y.dispersion.fsm.api;
    requires transitive com.github.f442y.dispersion.event.api;

    exports com.github.f442y.dispersion.routing.core;
    exports com.github.f442y.dispersion.routing.core.endpoint;
    exports com.github.f442y.dispersion.routing.core.policy;
    exports com.github.f442y.dispersion.routing.core.transport;
    exports com.github.f442y.dispersion.routing.core.worker;
}
