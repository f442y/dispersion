package com.github.f442y.dispersion.serialization.fory;

import com.github.f442y.dispersion.serialization.binary.BinarySerializationException;
import com.github.f442y.dispersion.serialization.binary.BinarySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class ForyBinarySerializerTest {

    private ForyBinarySerializer serializer;

    record Customer(UUID id, String name, String email) implements Serializable {}

    record OrderItem(String sku, int quantity, BigDecimal unitPrice) implements Serializable {}

    record OrderWorkflowContext(
            UUID orderId,
            Customer customer,
            List<OrderItem> items,
            BigDecimal totalAmount,
            Set<String> appliedDiscounts,
            Map<String, String> metadata,
            Instant createdAt
    ) implements Serializable {}

    sealed interface DomainSignal permits ApprovalSignal, RejectionSignal {}
    record ApprovalSignal(String approverId, Instant timestamp) implements DomainSignal, Serializable {}
    record RejectionSignal(String approverId, String reason, Instant timestamp) implements DomainSignal, Serializable {}

    record UnregisteredType(String value) {}

    @BeforeEach
    void setUp() {
        serializer = new ForyBinarySerializer();
        // Register types with explicit IDs for maximum speed and compact payloads
        serializer.register(Customer.class, 101);
        serializer.register(OrderItem.class, 102);
        serializer.register(OrderWorkflowContext.class, 103);
        serializer.register(ApprovalSignal.class, 104);
        serializer.register(RejectionSignal.class, 105);
    }

    @Test
    @DisplayName("ServiceLoader successfully discovers ForyBinarySerializer")
    void shouldDiscoverViaServiceLoader() {
        BinarySerializer discovered = BinarySerializer.load();
        assertThat(discovered).isNotNull();
        assertThat(discovered).isInstanceOf(ForyBinarySerializer.class);
    }

    @Test
    @DisplayName("Round-trip serialization of complex nested records with registered types")
    void shouldRoundTripComplexNestedRecords() {
        Customer customer = new Customer(UUID.randomUUID(), "Alice Smith", "alice@example.com");
        OrderItem item1 = new OrderItem("SKU-100", 2, new BigDecimal("49.99"));
        OrderItem item2 = new OrderItem("SKU-200", 1, new BigDecimal("19.50"));
        Instant now = Instant.parse("2026-09-19T03:00:00Z");

        OrderWorkflowContext original = new OrderWorkflowContext(
                UUID.randomUUID(),
                customer,
                List.of(item1, item2),
                new BigDecimal("119.48"),
                Set.of("SUMMER_SALE", "LOYALTY_10"),
                Map.of("channel", "web", "ipAddress", "192.168.1.1"),
                now
        );

        byte[] bytes = serializer.serialize(original);
        assertThat(bytes).isNotEmpty();

        OrderWorkflowContext restored = serializer.deserialize(bytes, OrderWorkflowContext.class);
        assertThat(restored.orderId()).isEqualTo(original.orderId());
        assertThat(restored.customer()).isEqualTo(customer);
        assertThat(restored.items()).containsExactly(item1, item2);
        assertThat(restored.totalAmount()).isEqualTo(original.totalAmount());
        assertThat(restored.appliedDiscounts()).containsExactlyInAnyOrder("SUMMER_SALE", "LOYALTY_10");
        assertThat(restored.metadata()).containsEntry("channel", "web");
        assertThat(restored.createdAt()).isEqualTo(now);
    }

    @Test
    @DisplayName("Round-trip serialization of polymorphic domain types")
    void shouldRoundTripPolymorphicDomainTypes() {
        Instant now = Instant.parse("2026-09-19T03:15:00Z");
        DomainSignal approval = new ApprovalSignal("mgr-42", now);
        DomainSignal rejection = new RejectionSignal("mgr-99", "Budget exceeded", now);

        byte[] approvalBytes = serializer.serialize(approval);
        Object restoredApproval = serializer.deserialize(approvalBytes);
        assertThat(restoredApproval).isInstanceOf(ApprovalSignal.class);
        assertThat(((ApprovalSignal) restoredApproval).approverId()).isEqualTo("mgr-42");

        byte[] rejectionBytes = serializer.serialize(rejection);
        DomainSignal restoredRejection = serializer.deserialize(rejectionBytes, DomainSignal.class);
        assertThat(restoredRejection).isInstanceOf(RejectionSignal.class);
        assertThat(((RejectionSignal) restoredRejection).reason()).isEqualTo("Budget exceeded");
    }

    @Test
    @DisplayName("Concurrent serialization across virtual threads")
    void shouldHandleHighConcurrencyAcrossVirtualThreads() throws InterruptedException, ExecutionException {
        try (java.util.concurrent.ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            int taskCount = 200;
            List<Future<Customer>> futures = new ArrayList<>(taskCount);

            for (int i = 0; i < taskCount; i++) {
                final int id = i;
                futures.add(executor.submit(() -> {
                    Customer original = new Customer(UUID.randomUUID(), "Customer-" + id, "user" + id + "@test.com");
                    byte[] payload = serializer.serialize(original);
                    return serializer.deserialize(payload, Customer.class);
                }));
            }

            for (int i = 0; i < taskCount; i++) {
                Customer result = futures.get(i).get();
                assertThat(result.name()).isEqualTo("Customer-" + i);
            }
        }
    }

    @Test
    @DisplayName("Fails securely when class registration is required and type is not registered")
    void shouldRejectUnregisteredClass() {
        UnregisteredType data = new UnregisteredType("secret");
        assertThatThrownBy(() -> serializer.serialize(data))
                .isInstanceOf(BinarySerializationException.class)
                .hasRootCauseInstanceOf(org.apache.fory.exception.InsecureException.class);
    }

    @Test
    @DisplayName("Throws BinarySerializationException on type mismatch")
    void shouldThrowOnTypeMismatch() {
        Customer customer = new Customer(UUID.randomUUID(), "Bob", "bob@example.com");
        byte[] bytes = serializer.serialize(customer);

        assertThatThrownBy(() -> serializer.deserialize(bytes, OrderItem.class))
                .isInstanceOf(BinarySerializationException.class)
                .hasMessageContaining("cannot be cast to target type");
    }

    @Test
    @DisplayName("Throws NullPointerException on null arguments")
    void shouldThrowOnNullArguments() {
        assertThatThrownBy(() -> serializer.serialize(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> serializer.deserialize(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> serializer.deserialize(new byte[0], null))
                .isInstanceOf(NullPointerException.class);
    }
}

