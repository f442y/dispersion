@Json.Import({
        ExecutionSummary.class,
        MachineDescriptor.class,
        SignalDeliveryResult.class,
        SignalRequest.class
})
package com.github.f442y.dispersion.serialization.avaje;

import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import io.avaje.jsonb.Json;
