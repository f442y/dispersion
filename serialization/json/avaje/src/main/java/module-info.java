module com.github.f442y.dispersion.serialization.avaje {
    requires static transitive org.jspecify;
    requires transitive com.github.f442y.dispersion.serialization.json;
    requires com.github.f442y.dispersion.event.api;
    requires com.github.f442y.dispersion.control.api;
    requires transitive io.avaje.jsonb;
    requires transitive io.avaje.json;

    exports com.github.f442y.dispersion.serialization.avaje;

    provides io.avaje.jsonb.spi.JsonbExtension
            with avaje.jsonb.GeneratedJsonComponent;

    provides com.github.f442y.dispersion.serialization.json.JsonSerializer
            with com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
}
