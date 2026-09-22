module com.github.f442y.dispersion.server.standalone {
    requires transitive com.github.f442y.dispersion.server.api;
    requires io.helidon.webserver;
    requires io.helidon.http;
    requires static org.jspecify;
    requires org.slf4j;

    exports com.github.f442y.dispersion.server.standalone;

    provides com.github.f442y.dispersion.server.api.ControlPlaneServerFactory
        with com.github.f442y.dispersion.server.standalone.HelidonControlPlaneServerFactory;
}
