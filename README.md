# Stow

Stow 是面向 Java 21 的高并发、多租户文件存储池。它以本地文件系统或操作系统已挂载的文件系统为物理存储，围绕文件放置、租户隔离、持久化队列、配额、恢复和运行维护提供统一的 Java API，并通过独立模块适配 Spring Boot。

## 主要能力

- 多租户隔离：租户元数据、队列、配额和文件路径相互隔离。
- 多存储卷：支持卷健康与容量探测、分片路径和候选卷选择。
- 持久化处理：以 journal 保存队列事实，以 SQLite 构建查询投影。
- 崩溃恢复：通过原子文件更新、幂等事件和恢复流程保护磁盘状态。
- 框架无关核心：`stow-core` 不依赖 Spring、CDI、Guice 或具体日志实现。
- 明确的资源所有权：运行时只关闭自身创建的资源，调用方注入的执行器仍由调用方管理。

Stow 的核心架构把物理文件、每租户 journal 和 SQLite 投影分别作为内容事实、队列事实和查询视图。完整设计与持久化约束见[总体设计](docs/stow-design.md)和[持久化与恢复契约](docs/stow-persistence-contract.md)。

## 环境要求

- OpenJDK 21
- Maven 3.9+

仓库包含 Maven Wrapper，不需要额外在仓库内安装 Maven。Windows 使用 `mvnw.cmd`，Linux 和 macOS 使用 `mvnw`。

## 引入依赖

当前开发版本为 `0.1.0-SNAPSHOT`。在仓库根目录执行一次本地安装：

```powershell
.\mvnw.cmd install
```

然后在 Maven 项目中引用核心模块：

```xml
<dependency>
  <groupId>io.github.cocosip</groupId>
  <artifactId>stow-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`stow-core` 只依赖 SLF4J API，应用程序可以自行选择兼容的日志实现。

## Java 使用

以下示例创建配置、启动运行时并读取预配置租户。`StowRuntime` 实现了 `AutoCloseable`，推荐始终使用 try-with-resources：

```java
import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.model.TenantContext;
import java.nio.file.Path;
import java.util.List;

StowConfiguration configuration = StowConfiguration.builder()
        .metadataDirectory(Path.of("data/metadata"))
        .quotaDirectory(Path.of("data/quota"))
        .queueDirectory(Path.of("data/queue"))
        .watcherDirectory(Path.of("data/watchers"))
        .autoCreateTenants(true)
        .preconfiguredTenants(List.of("tenant-a"))
        .volumes(List.of(new VolumeConfiguration(
                "volume-1",
                Path.of("data/volume-1"),
                2,
                64 * 1024,
                true)))
        .build();

try (StowRuntime runtime = Stow.open(configuration)) {
    TenantContext tenant = runtime.tenantManager().get("tenant-a");
    System.out.println(tenant.tenantId());
}
```

当前版本会校验卷配置，底层本地文件系统卷也已实现；完整 `StoragePool` 组合仍按[实现计划](docs/stow-implementation-plan.md)接入，因此上例只使用当前已接入运行时的租户 API。

## 配置

`StowConfiguration.builder()` 是框架无关的配置入口。常用配置包括：

| 分组 | 配置 | 说明 |
| --- | --- | --- |
| 路径 | `metadataDirectory` | 租户元数据与运行时锁目录，默认 `./stow-metadata` |
| 路径 | `quotaDirectory` | 配额数据目录，默认 `./stow-quota` |
| 路径 | `queueDirectory` | 持久化队列目录，默认 `./stow-queue` |
| 路径 | `watcherDirectory` | 文件监视状态目录，默认 `./stow-watchers` |
| 租户 | `autoCreateTenants` | 是否在首次访问时自动创建租户，默认关闭 |
| 租户 | `defaultQuota` | 新租户默认配额；`0` 表示不限制 |
| 租户 | `preconfiguredTenants` | 运行时启动时确保存在的租户列表 |
| 存储卷 | `volumes` | 一个或多个 `VolumeConfiguration` |

单个卷配置包含：

- `id`：非空卷标识。
- `mountPath`：卷挂载目录。
- `shardingDepth`：文件路径分片深度，范围为 `0..3`。
- `bufferSize`：I/O 缓冲区大小，必须为正数。
- `forceFlushAfterWrite`：原子提交前是否强制刷新文件内容。

所有路径都会在构建配置时转换为绝对规范路径。完整配置项、默认值和校验规则见[公共 API 与配置契约](docs/stow-api-contract.md)。

## 生命周期与资源所有权

- `Stow.open(configuration)` 等价于构建运行时并立即调用 `start()`。
- `Stow.builder().build()` 返回 `NEW` 状态，适合需要先完成依赖注入再显式启动的场景。
- 同一组运行时目录同一时间只能由一个 Stow 实例持有。
- Stow 会关闭自己创建的工作执行器、调度器和后台服务。
- 通过 builder 注入的执行器和调度器由调用方负责关闭。
- `close()` 可以重复调用；业务代码应优先使用 try-with-resources 保证释放目录锁和内部资源。

## 构建与验证

Windows：

```powershell
.\mvnw.cmd verify
```

Linux 或 macOS：

```bash
./mvnw verify
```

只验证核心模块：

```powershell
.\mvnw.cmd -pl stow-core clean verify
```

`verify` 会执行单元测试、集成测试阶段、JaCoCo 报告、Spotless 和 SpotBugs。部分符号链接安全测试在 Windows 账户没有创建符号链接权限时会按环境条件跳过。

## 模块

| 模块 | 用途 |
| --- | --- |
| `stow-core` | 框架无关的公共 API、SPI 和核心实现 |
| `stow-spring-boot-starter` | Spring Boot 配置绑定与生命周期适配 |
| `samples/stow-sample-console` | 控制台集成示例 |
| `samples/stow-sample-spring-boot` | Spring Boot 集成示例 |
| `benchmarks` | JMH 性能基准 |

正式发布制品只有 `io.github.cocosip:stow-core` 和 `io.github.cocosip:stow-spring-boot-starter`；samples 与 benchmarks 不参与发布。Starter、samples 和 benchmarks 目前保留为后续实现的模块骨架，使用前请核对[实现计划](docs/stow-implementation-plan.md)。

## 版本管理

整个 Maven reactor 使用根 POM 中的 `${revision}` 作为统一项目版本。第三方依赖版本集中在根 POM 的 `dependencyManagement`，构建插件版本也由根构建统一管理，子模块不单独声明版本。

发布或验证指定版本时只需覆盖一次 `revision`：

```powershell
.\mvnw.cmd -Drevision=1.0.0 clean verify
```

两个正式制品始终使用同一个版本号。详细约束见[构建与版本管理](docs/build-version-management.md)。

## 文档

- [开发文档索引](docs/README.md)
- [总体设计](docs/stow-design.md)
- [公共 API 与配置契约](docs/stow-api-contract.md)
- [持久化与恢复契约](docs/stow-persistence-contract.md)
- [构建与版本管理](docs/build-version-management.md)
- [分阶段实现计划](docs/stow-implementation-plan.md)

设计文档描述 Stow 1.0 的完整目标范围；尚未接入的能力及开发进度统一记录在实现计划中。
