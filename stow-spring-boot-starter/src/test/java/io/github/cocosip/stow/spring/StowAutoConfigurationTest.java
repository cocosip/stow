package io.github.cocosip.stow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.TenantManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class StowAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(StowAutoConfiguration.class)
            .withPropertyValues(
                    "stow.paths.metadata-directory=${java.io.tmpdir}/stow-test-metadata",
                    "stow.paths.quota-directory=${java.io.tmpdir}/stow-test-quota",
                    "stow.paths.queue-directory=${java.io.tmpdir}/stow-test-queue",
                    "stow.paths.watcher-directory=${java.io.tmpdir}/stow-test-watchers");

    @Test
    void createsOneRuntimeAndDoesNotExposeInternalComponents() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(StowRuntime.class);
            if (org.springframework.util.ClassUtils.isPresent(
                    "org.springframework.boot.actuate.health.HealthIndicator", context.getClassLoader())) {
                assertThat(context).hasBean("stowHealthContributor");
                assertThat(context).hasBean("stowHealthIndicator");
                assertThat(context.getBean("stowHealthIndicator").getClass().getInterfaces())
                        .anyMatch(type ->
                                type.getName().equals("org.springframework.boot.actuate.health.HealthIndicator"));
                try {
                    Class<?> healthType = Class.forName("org.springframework.boot.actuate.health.HealthIndicator");
                    assertThat(context.getBeanNamesForType(healthType)).contains("stowHealthIndicator");
                } catch (ClassNotFoundException exception) {
                    throw new AssertionError(exception);
                }
            } else {
                assertThat(context).doesNotHaveBean(StowHealthContributor.class);
            }
            assertThat(context).doesNotHaveBean("defaultStowRuntimeFactory");
            assertThat(context.getBean(StowRuntime.class).state().name()).isEqualTo("RUNNING");
        });
    }

    @Test
    void supportsConstructorInjectionOfPublicServices() {
        contextRunner
                .withUserConfiguration(PublicServiceConsumerConfiguration.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(PublicServiceConsumer.class);
                    assertThat(context.getBean(PublicServiceConsumer.class)
                                    .runtime()
                                    .state()
                                    .name())
                            .isEqualTo("RUNNING");
                    assertThat(context.getBean(PublicServiceConsumer.class).tenantManager())
                            .isNotNull();
                    assertThat(context.getBean(SecondPublicServiceConsumer.class)
                                    .runtime())
                            .isSameAs(
                                    context.getBean(PublicServiceConsumer.class).runtime())
                            .isSameAs(context.getBean(StowRuntime.class));
                });
    }

    @Test
    void bindsKebabCasePropertiesIntoCoreConfiguration() {
        contextRunner
                .withPropertyValues("stow.metadata.max-queue-size=42", "stow.journal.ack-mode=async")
                .run(context -> {
                    StowProperties properties = context.getBean(StowProperties.class);

                    assertThat(properties.getMetadata().getMaxQueueSize()).isEqualTo(42);
                    assertThat(properties.getJournal().getAckMode().name()).isEqualTo("ASYNC");
                });
    }

    @Test
    void rejectsInvalidBoundPropertyDuringContextCreation() {
        contextRunner
                .withPropertyValues("stow.metadata.soft-merge-threshold-percent=101")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(
                                    io.github.cocosip.stow.exception.InvalidConfigurationException.class);
                });
    }

    @Test
    void usesUserClockBeanForRuntimeHealth() {
        Instant now = Instant.parse("2026-01-02T03:04:05Z");
        contextRunner.withUserConfiguration(FixedClockConfiguration.class).run(context -> {
            StowRuntime runtime = context.getBean(StowRuntime.class);

            assertThat(runtime.health().components().get("runtime").checkedAt()).isEqualTo(now);
        });
    }

    @Test
    void doesNotRegisterActuatorContributorWhenActuatorIsAbsent() {
        contextRunner
                .withClassLoader(new FilteredClassLoader("org.springframework.boot.actuate.health.HealthIndicator"))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(StowHealthContributor.class);
                });
    }

    @Test
    void doesNotRegisterActuatorContributorWhenDisabled() {
        contextRunner.withPropertyValues("stow.actuator.enabled=false").run(context -> {
            assertThat(context).doesNotHaveBean(StowHealthContributor.class);
        });
    }

    @Test
    void respectsStandardActuatorHealthDisableProperty() {
        contextRunner.withPropertyValues("management.health.stow.enabled=false").run(context -> {
            assertThat(context).doesNotHaveBean(StowHealthContributor.class);
        });
    }

    @Test
    void doesNotRegisterMeterBinderWhenMicrometerIsAbsent() {
        contextRunner
                .withClassLoader(new FilteredClassLoader(MeterRegistry.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(StowMeterBinder.class);
                });
    }

    @Test
    void doesNotRegisterMeterBinderWhenDisabled() {
        contextRunner.withPropertyValues("stow.metrics.enabled=false").run(context -> {
            assertThat(context).doesNotHaveBean(StowMeterBinder.class);
        });
    }

    @Test
    void registersMeterBinderWhenMicrometerRegistryIsAvailable() {
        contextRunner.withUserConfiguration(MeterConfiguration.class).run(context -> {
            assertThat(context).hasSingleBean(StowMeterBinder.class);
            MeterRegistry registry = context.getBean(MeterRegistry.class);
            context.getBean(StowMeterBinder.class).bindTo(registry);
            assertThat(registry.find("stow.runtime.state").gauge()).isNotNull();
        });
    }

    @Test
    void shutsRuntimeDownWhenContextCloses() {
        AtomicReference<StowRuntime> runtime = new AtomicReference<>();

        contextRunner.run(context -> runtime.set(context.getBean(StowRuntime.class)));

        assertThat(runtime)
                .hasValueSatisfying(value -> assertThat(value.state().name()).isEqualTo("TERMINATED"));
    }

    @Configuration(proxyBeanMethods = false)
    static class FixedClockConfiguration {

        @Bean
        Clock clock() {
            return Clock.fixed(Instant.parse("2026-01-02T03:04:05Z"), ZoneOffset.UTC);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MeterConfiguration {

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PublicServiceConsumerConfiguration {

        @Bean
        PublicServiceConsumer publicServiceConsumer(StowRuntime runtime, TenantManager tenantManager) {
            return new PublicServiceConsumer(runtime, tenantManager);
        }

        @Bean
        SecondPublicServiceConsumer secondPublicServiceConsumer(StowRuntime runtime) {
            return new SecondPublicServiceConsumer(runtime);
        }
    }

    record PublicServiceConsumer(StowRuntime runtime, TenantManager tenantManager) {}

    record SecondPublicServiceConsumer(StowRuntime runtime) {}
}
