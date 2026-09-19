package io.github.cocosip.stow.spring;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.StowRuntime;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

/** Starts every Spring-managed Stow runtime before dependent beans are created. */
final class StowRuntimeStartup implements BeanPostProcessor {

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof StowRuntime runtime && runtime.state() == RuntimeState.NEW) {
            runtime.start();
        }
        return bean;
    }
}
