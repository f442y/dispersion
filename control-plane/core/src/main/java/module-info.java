module com.github.f442y.dispersion.control.core {
    requires static transitive org.jspecify;
    requires org.slf4j;
    requires transitive com.github.f442y.dispersion.control.api;
    requires transitive com.github.f442y.dispersion.event.api;

    exports com.github.f442y.dispersion.control.core;

    provides com.github.f442y.dispersion.control.ControlPlaneProvider
            with com.github.f442y.dispersion.control.core.DefaultControlPlaneProvider;
}
