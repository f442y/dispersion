module com.github.f442y.dispersion.fsm.api {
    requires static org.jspecify;
    requires transitive com.github.f442y.dispersion.event.api;

    exports com.github.f442y.dispersion.fsm;
    exports com.github.f442y.dispersion.fsm.config;
    exports com.github.f442y.dispersion.fsm.context;
    exports com.github.f442y.dispersion.fsm.exception;
    exports com.github.f442y.dispersion.fsm.executor;
    exports com.github.f442y.dispersion.fsm.state;
}
