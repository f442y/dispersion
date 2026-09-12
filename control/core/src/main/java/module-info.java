module com.github.f442y.dispersion.control.core {
    requires static org.jspecify;
    requires org.slf4j;
    requires transitive com.github.f442y.dispersion.control.api;
    requires transitive com.github.f442y.dispersion.event.core;
    requires transitive com.github.f442y.dispersion.fsm.core;
    requires transitive com.github.f442y.dispersion.orchestration.core;

    exports com.github.f442y.dispersion.control.core;
}
