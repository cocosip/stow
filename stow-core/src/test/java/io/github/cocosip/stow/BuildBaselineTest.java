package io.github.cocosip.stow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.helpers.DefaultHandler;

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
        assertThat(pluginVersionsInChildren()).isEmpty();
        assertThat(deploySkippedModules())
                .containsExactlyInAnyOrder("stow-sample-console", "stow-sample-spring-boot", "stow-benchmarks");
    }

    @Test
    void discoversNestedReactorModules(@TempDir Path temporaryReactor) throws Exception {
        writePom(temporaryReactor, "pom.xml", "root", List.of("stow-core", "nested"));
        writePom(temporaryReactor, "stow-core/pom.xml", "stow-core", List.of());
        writePom(temporaryReactor, "nested/pom.xml", "nested-parent", List.of("leaf"));
        writePom(temporaryReactor, "nested/leaf/pom.xml", "nested-leaf", List.of());

        assertThat(childProjects(temporaryReactor))
                .extracting(project -> childText(project, "artifactId"))
                .containsExactlyInAnyOrder("stow-core", "nested-parent", "nested-leaf");
    }

    @Test
    void detectsVersionsOutsideRootManagement(@TempDir Path temporaryReactor) throws Exception {
        Path childPom = temporaryReactor.resolve("pom.xml");
        Files.writeString(
                childPom,
                """
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <modelVersion>4.0.0</modelVersion>
                  <artifactId>child</artifactId>
                  <dependencies>
                    <dependency>
                      <groupId>io.github.cocosip</groupId>
                      <artifactId>stow-core</artifactId>
                      <version>${project.version}</version>
                    </dependency>
                  </dependencies>
                  <dependencyManagement>
                    <dependencies>
                      <dependency>
                        <groupId>org.example</groupId>
                        <artifactId>managed-library</artifactId>
                        <version>1.2.3</version>
                      </dependency>
                    </dependencies>
                  </dependencyManagement>
                  <build>
                    <plugins>
                      <plugin>
                        <artifactId>direct-plugin</artifactId>
                        <version>2.0</version>
                      </plugin>
                    </plugins>
                    <pluginManagement>
                      <plugins>
                        <plugin>
                          <artifactId>managed-plugin</artifactId>
                          <version>3.0</version>
                        </plugin>
                      </plugins>
                    </pluginManagement>
                  </build>
                  <profiles>
                    <profile>
                      <id>version-drift</id>
                      <dependencies>
                        <dependency>
                          <groupId>org.example</groupId>
                          <artifactId>profile-library</artifactId>
                          <version>4.0</version>
                        </dependency>
                      </dependencies>
                      <build>
                        <pluginManagement>
                          <plugins>
                            <plugin>
                              <artifactId>profile-plugin</artifactId>
                              <version>5.0</version>
                            </plugin>
                          </plugins>
                        </pluginManagement>
                      </build>
                    </profile>
                  </profiles>
                </project>
                """);
        Element childProject = parseProject(childPom);

        assertThat(thirdPartyDependencyVersions(List.of(childProject))).containsExactly("1.2.3", "4.0");
        assertThat(reactorDependencyVersions(List.of(childProject))).containsExactly("${project.version}");
        assertThat(pluginVersions(List.of(childProject))).containsExactlyInAnyOrder("2.0", "3.0", "5.0");
    }

    @Test
    void rejectsDoctypeDeclarations(@TempDir Path temporaryReactor) throws Exception {
        Path secret = temporaryReactor.resolve("secret.txt");
        Files.writeString(secret, "must-not-be-read");
        Path pom = temporaryReactor.resolve("pom.xml");
        Files.writeString(
                pom,
                """
                <?xml version="1.0"?>
                <!DOCTYPE project [<!ENTITY secret SYSTEM "%s">]>
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <modelVersion>4.0.0</modelVersion>
                  <artifactId>&secret;</artifactId>
                </project>
                """
                        .formatted(secret.toUri()));

        assertThatThrownBy(() -> parseProject(pom))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to parse");
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
        return thirdPartyDependencyVersions(childProjects());
    }

    private static List<String> thirdPartyDependencyVersions(List<Element> projects) {
        return projects.stream()
                .flatMap(project -> dependencies(project).stream())
                .filter(dependency -> !"io.github.cocosip".equals(childText(dependency, "groupId")))
                .map(dependency -> child(dependency, "version"))
                .filter(element -> element != null)
                .map(Element::getTextContent)
                .toList();
    }

    private List<String> reactorDependencyVersions() throws Exception {
        return reactorDependencyVersions(childProjects());
    }

    private static List<String> reactorDependencyVersions(List<Element> projects) {
        return projects.stream()
                .flatMap(project -> dependencies(project).stream())
                .filter(dependency -> "io.github.cocosip".equals(childText(dependency, "groupId")))
                .map(dependency -> childText(dependency, "version"))
                .toList();
    }

    private static List<String> pluginVersions(List<Element> projects) {
        return projects.stream()
                .flatMap(project -> plugins(project).stream())
                .map(plugin -> child(plugin, "version"))
                .filter(element -> element != null)
                .map(Element::getTextContent)
                .toList();
    }

    private List<String> pluginVersionsInChildren() throws Exception {
        return pluginVersions(childProjects());
    }

    private List<String> deploySkippedModules() throws Exception {
        return childProjects().stream()
                .filter(project -> "true".equals(childText(child(project, "properties"), "maven.deploy.skip")))
                .map(project -> childText(project, "artifactId"))
                .toList();
    }

    private List<Element> childProjects() throws Exception {
        return childProjects(repositoryRoot());
    }

    private static List<Element> childProjects(Path root) {
        List<Element> projects = reactorProjects(root);
        return projects.subList(1, projects.size());
    }

    private List<Element> reactorProjects() throws Exception {
        return reactorProjects(repositoryRoot());
    }

    private static List<Element> reactorProjects(Path root) {
        List<Element> projects = new ArrayList<>();
        collectReactorProjects(root.resolve("pom.xml"), projects);
        return projects;
    }

    private static void collectReactorProjects(Path pom, List<Element> projects) {
        Element project = parseProject(pom);
        projects.add(project);
        Element modules = child(project, "modules");
        if (modules == null) {
            return;
        }
        for (Node node = modules.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element module && "module".equals(module.getLocalName())) {
                Path modulePath =
                        pom.getParent().resolve(module.getTextContent().trim());
                Path modulePom = Files.isDirectory(modulePath) ? modulePath.resolve("pom.xml") : modulePath;
                collectReactorProjects(modulePom, projects);
            }
        }
    }

    private Element rootProject() throws Exception {
        return reactorProjects().getFirst();
    }

    private static Element parseProject(Path pom) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            var builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new DefaultHandler());
            Document document = builder.parse(pom.toFile());
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

    private static void writePom(Path reactorRoot, String relativePath, String artifactId, List<String> modules)
            throws IOException {
        String moduleDeclarations = modules.stream()
                .map(module -> "<module>" + module + "</module>")
                .reduce("", String::concat);
        String modulesElement = modules.isEmpty() ? "" : "<modules>" + moduleDeclarations + "</modules>";
        Path pom = reactorRoot.resolve(relativePath);
        Files.createDirectories(pom.getParent());
        Files.writeString(
                pom,
                "<project xmlns=\"http://maven.apache.org/POM/4.0.0\"><modelVersion>4.0.0</modelVersion>"
                        + "<artifactId>"
                        + artifactId
                        + "</artifactId>"
                        + modulesElement
                        + "</project>");
    }

    private static List<Element> dependencies(Element project) {
        List<Element> result = new ArrayList<>();
        for (Element scope : versionScopes(project)) {
            addChildren(result, child(scope, "dependencies"), "dependency");
            addChildren(result, child(child(scope, "dependencyManagement"), "dependencies"), "dependency");
        }
        return result;
    }

    private static List<Element> plugins(Element project) {
        List<Element> result = new ArrayList<>();
        for (Element scope : versionScopes(project)) {
            Element build = child(scope, "build");
            addChildren(result, child(build, "plugins"), "plugin");
            addChildren(result, child(child(build, "pluginManagement"), "plugins"), "plugin");
        }
        return result;
    }

    private static List<Element> versionScopes(Element project) {
        List<Element> scopes = new ArrayList<>();
        scopes.add(project);
        addChildren(scopes, child(project, "profiles"), "profile");
        return scopes;
    }

    private static void addChildren(List<Element> result, Element parent, String name) {
        if (parent == null) {
            return;
        }
        for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node instanceof Element element && name.equals(element.getLocalName())) {
                result.add(element);
            }
        }
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
