module com.github.f442y.dispersion.orchestration.batch {
    requires transitive com.github.f442y.dispersion.event.api;
    requires transitive com.github.f442y.dispersion.fsm.api;
    requires transitive com.github.f442y.dispersion.control.api;
    requires transitive com.github.f442y.dispersion.orchestration.api;
    requires org.slf4j;
    requires static transitive org.jspecify;

    exports com.github.f442y.dispersion.orchestration.batch;
}
