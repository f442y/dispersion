package com.github.f442y.dispersion.control;

import org.jspecify.annotations.NonNull;

/**
 * Service provider interface (SPI) for creating {@link ControlPlane} instances.
 */
@FunctionalInterface
public interface ControlPlaneProvider {

    /**
     * Creates and returns a new {@link ControlPlane} instance.
     *
     * @return a new ControlPlane instance
     */
    @NonNull
    ControlPlane create();
}
