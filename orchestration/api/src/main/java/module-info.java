module com.github.f442y.dispersion.orchestration.api {
    requires static org.jspecify;
    requires transitive com.github.f442y.dispersion.fsm.api;
    requires transitive com.github.f442y.dispersion.event.api;

    exports com.github.f442y.dispersion.orchestration;
    exports com.github.f442y.dispersion.orchestration.batch;
    exports com.github.f442y.dispersion.orchestration.command;
    exports com.github.f442y.dispersion.orchestration.messaging;
}
