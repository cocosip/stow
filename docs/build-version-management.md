# Stow 构建与版本管理

## 目标

Stow 的 Maven reactor 使用一个项目版本和一组集中管理的依赖、插件版本。任何模块不得单独决定对外版本，发布时 `stow-core` 与 `stow-spring-boot-starter` 必须使用相同版本。

## 项目版本

根 POM 使用 Maven CI-friendly 变量 `${revision}` 作为项目版本，并在根 POM 的 `properties` 中提供开发默认值 `0.1.0-SNAPSHOT`。

所有子模块：

- 通过 `${revision}` 引用父 POM；
- 不声明自己的独立项目版本；
- 引用 reactor 内其他模块时使用 `${project.version}`；
- 继承相同的 `groupId` 与项目版本。

因此日常开发只修改根 POM 的 `revision`。发布构建可以通过
`.\mvnw.cmd -Drevision=<version> clean deploy` 覆盖版本，而不修改多个模块 POM。

## 消费端 POM

发布模块使用 `flatten-maven-plugin` 的 `resolveCiFriendliesOnly` 模式：

- 构建源码继续保留可维护的 `${revision}`；
- `install`、`deploy` 使用解析为具体版本的扁平 POM；
- 依赖、可选依赖、scope 和其他消费端元数据保持不变；
- `clean` 删除生成的扁平 POM。

该配置由根 POM 统一继承。samples 与 benchmarks 仍设置
`maven.deploy.skip=true`，正式发布物保持为 `stow-core` 和
`stow-spring-boot-starter` 两个 JAR。

## 依赖与插件版本

所有第三方依赖版本只在根 POM 的 `dependencyManagement` 中声明。子模块选择依赖和 scope，不写版本。

所有构建插件版本只在根 POM 的 `pluginManagement` 或根构建插件声明中定义。子模块只添加模块特有配置，不复制插件版本。

引入新依赖或插件时，必须先在根 POM 增加版本属性和管理项，再在子模块引用。

## 防漂移验证

构建基线测试解析 reactor POM，并验证：

- 根项目版本为 `${revision}`，且存在唯一开发默认值；
- 每个子模块父版本为 `${revision}`；
- 子模块自身没有独立项目版本；
- reactor 内依赖只使用 `${project.version}`；
- 子模块第三方依赖不声明版本；
- 两个正式发布模块的有效版本相同；
- samples 与 benchmarks 保持禁止部署。

完整 `mvnw verify` 还必须验证扁平 POM 能解析为指定的具体版本，并继续执行现有格式、测试和静态分析门禁。

## 发布约束

发布版本必须是不带 `SNAPSHOT` 的合法 Maven 版本。发布流水线在一次 reactor 构建中传入唯一的 `revision`，不得分别发布两个正式模块或为它们指定不同版本。
