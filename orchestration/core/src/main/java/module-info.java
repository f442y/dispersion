module com.github.f442y.dispersion.orchestration.core {
    requires static org.jspecify;
    requires org.slf4j;
    requires transitive com.github.f442y.dispersion.orchestration.api;
    requires transitive com.github.f442y.dispersion.control.api;
    requires transitive com.github.f442y.dispersion.fsm.core;

    exports com.github.f442y.dispersion.orchestration.core;
    exports com.github.f442y.dispersion.orchestration.core.batch;
    exports com.github.f442y.dispersion.orchestration.core.messaging;
}
