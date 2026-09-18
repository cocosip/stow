package io.github.cocosip.stow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

class BuildBaselineTest {

    @Test
    void runsOnJava21() {
        assertThat(Runtime.version().feature()).isEqualTo(21);
    }

    @Test
    void loadsSlf4jApiWithoutBundledBinding() {
        assertThat(LoggerFactory.getILoggerFactory()).isNotNull();
        assertThatThrownBy(() -> Class.forName("org.slf4j.impl.StaticLoggerBinder"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void keepsReactorVersionsCentralized() throws Exception {
        assertThat(rootProjectVersion()).isEqualTo("${revision}");
        assertThat(rootProperty("revision")).isEqualTo("0.1.0-SNAPSHOT");
        assertThat(childParentVersions()).containsOnly("${revision}");
        assertThat(childDeclaredProjectVersions()).isEmpty();
        assertThat(thirdPartyDependencyVersionsInChildren()).isEmpty();
        assertThat(reactorDependencyVersions()).containsOnly("${project.version}");
        assertThat(deploySkippedModules())
                .containsExactlyInAnyOrder("stow-sample-console", "stow-sample-spring-boot", "stow-benchmarks");
    }

    private String rootProjectVersion() throws Exception {
        return childText(rootProject(), "version");
    }

    private String rootProperty(String propertyName) throws Exception {
        return childText(child(rootProject(), "properties"), propertyName);
    }

    private List<String> childParentVersions() throws Exception {
        return childProjects().stream()
                .map(project -> childText(child(project, "parent"), "version"))
                .toList();
    }

    private List<String> childDeclaredProjectVersions() throws Exception {
        return childProjects().stream()
                .map(project -> child(project, "version"))
                .filter(element -> element != null)
                .map(Element::getTextContent)
                .toList();
    }

    private List<String> thirdPartyDependencyVersionsInChildren() throws Exception {
        return childProjects().stream()
                .flatMap(project -> dependencies(project).stream())
                .filter(dependency -> !"io.github.cocosip".equals(childText(dependency, "groupId")))
                .map(dependency -> child(dependency, "version"))
                .filter(element -> element != null)
                .map(Element::getTextContent)
                .toList();
    }

    private List<String> reactorDependencyVersions() throws Exception {
        return childProjects().stream()
                .flatMap(project -> dependencies(project).stream())
                .filter(dependency -> "io.github.cocosip".equals(childText(dependency, "groupId")))
                .map(dependency -> childText(dependency, "version"))
                .toList();
    }

    private List<String> deploySkippedModules() throws Exception {
        return childProjects().stream()
                .filter(project -> "true".equals(childText(child(project, "properties"), "maven.deploy.skip")))
                .map(project -> childText(project, "artifactId"))
                .toList();
    }

    private List<Element> childProjects() throws Exception {
        return reactorProjects().subList(1, reactorProjects().size());
    }

    private List<Element> reactorProjects() throws Exception {
        Path root = repositoryRoot();
        return List.of(
                        root.resolve("pom.xml"),
                        root.resolve("stow-core/pom.xml"),
                        root.resolve("stow-spring-boot-starter/pom.xml"),
                        root.resolve("samples/stow-sample-console/pom.xml"),
                        root.resolve("samples/stow-sample-spring-boot/pom.xml"),
                        root.resolve("benchmarks/pom.xml"))
                .stream()
                .map(BuildBaselineTest::parseProject)
                .toList();
    }

    private Element rootProject() throws Exception {
        return reactorProjects().getFirst();
    }

    private static Element parseProject(Path pom) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            Document document = factory.newDocumentBuilder().parse(pom.toFile());
            return document.getDocumentElement();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to parse " + pom, exception);
        }
    }

    private static Path repositoryRoot() throws IOException {
        Path directory = Path.of("").toAbsolutePath();
        while (directory != null) {
            if (Files.isRegularFile(directory.resolve("pom.xml"))
                    && Files.isDirectory(directory.resolve("stow-core"))) {
                return directory;
            }
            directory = directory.getParent();
        }
        throw new IllegalStateException("Unable to find the reactor root");
    }

    private static List<Element> dependencies(Element project) {
        Element dependencies = child(project, "dependencies");
        if (dependencies == null) {
            return List.of();
        }
        List<Element> result = new ArrayList<>();
        for (Node node = dependencies.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element element && "dependency".equals(element.getLocalName())) {
                result.add(element);
            }
        }
        return result;
    }

    private static Element child(Element parent, String name) {
        if (parent == null) {
            return null;
        }
        for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element element && name.equals(element.getLocalName())) {
                return element;
            }
        }
        return null;
    }

    private static String childText(Element parent, String name) {
        Element child = child(parent, name);
        return child == null ? null : child.getTextContent();
    }
}
