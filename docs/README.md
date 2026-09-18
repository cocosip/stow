# Stow 开发文档

本文档目录是 Stow 1.0 的开发依据。实现时按以下顺序阅读：

1. [仓库 README](../README.md)：项目介绍、依赖接入、Java 使用、配置、构建和模块说明。
2. [总体设计](stow-design.md)：目标、架构、完整功能范围与完成定义。
3. [公共 API 与配置契约](stow-api-contract.md)：公开类型、生命周期、SPI、配置项和默认值。
4. [持久化与恢复契约](stow-persistence-contract.md)：目录布局、journal、SQLite schema、状态迁移和恢复不变量。
5. [实现计划](stow-implementation-plan.md)：测试先行的任务顺序、文件、命令、提交边界和阶段进度。
6. [构建与版本管理](build-version-management.md)：统一项目版本、依赖/插件版本和发布 POM 规则。
7. [日志](logging.md)：核心日志门面、provider 选择和 SLF4J 2.x 兼容 profile。

设计、API、持久化和实现计划共同构成 Stow 1.0 基线。若实现与文档冲突，先修正文档并说明原因，再修改代码。任何 Locus 2.0 对齐项都不得以“后续补充”为由移出 1.0。

## 当前实现状态

截至 2026-09-18，Task 1-5 已完成并通过各阶段测试与独立走查；下一阶段是 Task 6。设计文档描述完整 1.0 目标，不代表其中所有能力已经实现。当前可用范围、验证结果和后续任务必须以[实现计划](stow-implementation-plan.md)的勾选状态为准，在 Task 18 的完整发布门禁通过前不得声明 1.0 完成。
