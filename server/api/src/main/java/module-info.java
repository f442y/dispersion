module com.github.f442y.dispersion.server.api {
    requires transitive com.github.f442y.dispersion.control.api;
    requires transitive com.github.f442y.dispersion.serialization.json;
    requires static org.jspecify;

    exports com.github.f442y.dispersion.server.api;

    uses com.github.f442y.dispersion.server.api.ControlPlaneServerFactory;
}
