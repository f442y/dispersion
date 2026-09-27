module com.github.f442y.dispersion.orchestration.messaging {
    requires transitive com.github.f442y.dispersion.orchestration.api;
    requires org.slf4j;
    requires static transitive org.jspecify;

    exports com.github.f442y.dispersion.orchestration.messaging.broker;
}
