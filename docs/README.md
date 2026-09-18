# Stow 开发文档

本文档目录是 Stow 1.0 的开发依据。实现时按以下顺序阅读：

1. [总体设计](stow-design.md)：目标、架构、完整功能范围与完成定义。
2. [公共 API 与配置契约](stow-api-contract.md)：公开类型、生命周期、SPI、配置项和默认值。
3. [持久化与恢复契约](stow-persistence-contract.md)：目录布局、journal、SQLite schema、状态迁移和恢复不变量。
4. [实现计划](stow-implementation-plan.md)：测试先行的任务顺序、文件、命令和提交边界。

四份文档共同构成 Stow 1.0 基线。若实现与文档冲突，先修正文档并说明原因，再修改代码。任何 Locus 2.0 对齐项都不得以“后续补充”为由移出 1.0。
