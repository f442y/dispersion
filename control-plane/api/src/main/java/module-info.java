module com.github.f442y.dispersion.control.api {
    requires static org.jspecify;
    requires transitive com.github.f442y.dispersion.event.api;
    requires transitive com.github.f442y.dispersion.routing.api;

    exports com.github.f442y.dispersion.control;
}
