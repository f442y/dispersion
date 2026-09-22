module com.github.f442y.dispersion.serialization.json {
    requires static transitive org.jspecify;
    requires transitive com.github.f442y.dispersion.event.api;
    requires transitive com.github.f442y.dispersion.control.api;

    exports com.github.f442y.dispersion.serialization.json;

    uses com.github.f442y.dispersion.serialization.json.JsonSerializer;
}
