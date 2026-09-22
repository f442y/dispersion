module com.github.f442y.dispersion.server.jakarta {
    requires transitive com.github.f442y.dispersion.server.api;
    requires transitive jakarta.ws.rs;
    requires static org.jspecify;
    requires org.slf4j;

    exports com.github.f442y.dispersion.server.jakarta;
}
