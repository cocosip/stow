package io.github.cocosip.stow.spring;

import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.model.ComponentHealth;
import io.github.cocosip.stow.model.RuntimeHealth;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Bridges the framework-neutral runtime health snapshot to an optional Actuator API. */
public final class StowHealthContributor {

    private final StowRuntime runtime;

    public StowHealthContributor(StowRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    public Map<String, Object> details() {
        RuntimeHealth runtimeHealth = runtime.health();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("status", runtimeHealth.status().name());
        runtimeHealth.components().forEach((name, component) -> details.put(name, componentDetails(component)));
        return Map.copyOf(details);
    }

    /** Creates an Actuator HealthIndicator proxy without linking this starter to one Boot generation. */
    Object asHealthIndicator() {
        try {
            Class<?> indicatorType = Class.forName("org.springframework.boot.actuate.health.HealthIndicator");
            return Proxy.newProxyInstance(
                    indicatorType.getClassLoader(), new Class<?>[] {indicatorType}, new HealthIndicatorHandler(this));
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Actuator health API is not available", exception);
        }
    }

    private static Map<String, Object> componentDetails(ComponentHealth component) {
        return Map.of(
                "status", component.status().name(),
                "summary", component.summary(),
                "checkedAt", component.checkedAt().toString());
    }

    private static final class HealthIndicatorHandler implements InvocationHandler {
        private final StowHealthContributor contributor;

        private HealthIndicatorHandler(StowHealthContributor contributor) {
            this.contributor = contributor;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
            return switch (method.getName()) {
                case "health" -> health();
                case "toString" -> "StowHealthIndicator";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == arguments[0];
                default -> throw new UnsupportedOperationException(method.toGenericString());
            };
        }

        private Object health() throws ReflectiveOperationException {
            Class<?> healthType = Class.forName("org.springframework.boot.actuate.health.Health");
            Map<String, Object> details = contributor.details();
            Object builder =
                    switch (details.get("status").toString()) {
                        case "UP" -> healthType.getMethod("up").invoke(null);
                        case "DOWN" -> healthType.getMethod("down").invoke(null);
                        default -> healthType.getMethod("status", String.class).invoke(null, details.get("status"));
                    };
            Method withDetail = builder.getClass().getMethod("withDetail", String.class, Object.class);
            for (Map.Entry<String, Object> detail : details.entrySet()) {
                withDetail.invoke(builder, detail.getKey(), detail.getValue());
            }
            return builder.getClass().getMethod("build").invoke(builder);
        }
    }
}
