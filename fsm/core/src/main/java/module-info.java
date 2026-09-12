module com.github.f442y.dispersion.fsm.core {
    requires static org.jspecify;
    requires org.slf4j;
    requires transitive com.github.f442y.dispersion.fsm.api;
    requires transitive com.github.f442y.dispersion.event.core;

    exports com.github.f442y.dispersion.fsm.core;
    exports com.github.f442y.dispersion.fsm.core.atomic;
    exports com.github.f442y.dispersion.fsm.core.builder;
    exports com.github.f442y.dispersion.fsm.core.context;
    exports com.github.f442y.dispersion.fsm.core.executor;
    exports com.github.f442y.dispersion.fsm.core.state;
}
