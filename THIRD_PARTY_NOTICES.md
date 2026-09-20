# 第三方软件说明

本项目使用开源库实现协议与配置处理，不包含个人代理配置、订阅、认证凭据或网关数据。

## 上游项目

本仓库由 NanSsye 基于 [gylive/ccodex-sleep-state](https://github.com/gylive/ccodex-sleep-state) 二次开发。原作者及原贡献者的版权、提交历史和 GPL-3.0 许可证予以保留；本仓库的新增功能、修复和发行由 NanSsye 维护。本文及发布包不代表上游作者或依赖项目为二次开发版本背书。

## 直接依赖

- [Mihomo](https://github.com/MetaCubeX/mihomo)：出站协议适配和订阅 URI 转换，采用 GPL-3.0。这也是本项目采用 GPL-3.0、而不是宽松许可证的原因。
- [go-toml v2](https://github.com/pelletier/go-toml)：校验 TOML，并提供语法位置，避免重写无关配置。采用 MIT 许可证。
- [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys)：提供 Windows 文件锁等系统接口。采用 BSD 类许可证。
- [yaml.v3](https://github.com/go-yaml/yaml/tree/v3)：解析 YAML 订阅。具体 MIT / Apache-2.0 条款见依赖自带的许可证文件。

直接与间接依赖的准确版本记录在 `go.mod` 和 `go.sum`。发布工作流附带包含 vendored Go 依赖及其许可证的对应源码包；二进制压缩包附带本说明与项目的 GPL 许可证。上述项目并未因此为本项目提供背书。

需要自行准备完整源码包时，在干净的仓库副本中运行 `go mod vendor`。再分发源代码或二进制时，请保留上游版权和许可证文件，并同时遵守各依赖的声明及 GPL 的源码提供义务。

`LICENSE` 保留 GNU GPL 的官方英文原文，不用非官方翻译替代法律文本。使用教程与项目介绍均使用中文。
