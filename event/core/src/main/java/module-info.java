module com.github.f442y.dispersion.event.core {
    requires static org.jspecify;
    requires org.slf4j;
    requires transitive com.github.f442y.dispersion.event.api;

    exports com.github.f442y.dispersion.event.dispatcher;
}
