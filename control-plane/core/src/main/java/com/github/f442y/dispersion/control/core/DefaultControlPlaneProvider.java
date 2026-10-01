package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ControlPlaneProvider;
import org.jspecify.annotations.NonNull;

/**
 * Default {@link ControlPlaneProvider} providing instances of {@link DefaultControlPlane}.
 */
public final class DefaultControlPlaneProvider implements ControlPlaneProvider {

    public DefaultControlPlaneProvider() {}

    @Override
    public @NonNull ControlPlane create() {
        return new DefaultControlPlane();
    }
}
