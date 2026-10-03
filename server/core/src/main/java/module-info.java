module com.github.f442y.dispersion.server.core {
    requires transitive com.github.f442y.dispersion.server.api;
    requires static transitive org.jspecify;
    requires org.slf4j;

    exports com.github.f442y.dispersion.server.core;
}
