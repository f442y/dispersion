package com.github.f442y.dispersion.server.spring;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Spring Web configuration registering {@link DispersionControlPlaneController}
 * and {@link DispersionWebDashboardController} into the application context.
 */
@Configuration
@Import({DispersionControlPlaneController.class, DispersionWebDashboardController.class})
public class DispersionSpringWebConfiguration {
}
