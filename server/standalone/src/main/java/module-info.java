module com.github.f442y.dispersion.server.standalone {
    requires transitive com.github.f442y.dispersion.server.api;
    requires io.helidon.webserver;
    requires io.helidon.webserver.staticcontent;
    requires io.helidon.http;
    requires static transitive org.jspecify;
    requires org.slf4j;

    exports com.github.f442y.dispersion.server.standalone;

    uses com.github.f442y.dispersion.control.ControlPlaneProvider;
    uses com.github.f442y.dispersion.serialization.json.JsonSerializer;

    provides com.github.f442y.dispersion.server.api.ControlPlaneServerFactory
        with com.github.f442y.dispersion.server.standalone.HelidonControlPlaneServerFactory;
}
