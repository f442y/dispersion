module com.github.f442y.dispersion.serialization.fory {
    requires static transitive org.jspecify;
    requires transitive com.github.f442y.dispersion.serialization.binary;
    requires transitive org.apache.fory.core;

    exports com.github.f442y.dispersion.serialization.fory;

    provides com.github.f442y.dispersion.serialization.binary.BinarySerializer
            with com.github.f442y.dispersion.serialization.fory.ForyBinarySerializer;
}
