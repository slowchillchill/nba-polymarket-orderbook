# 部署和运行

在独立目录部署固定已审 commit。执行 `sudo deploy/install.sh <完整commit>` 后，安装脚本不会启用扫描定时器。部署前将模板 `deploy/config.example.toml` 复制为 `/etc/nba-orderbook/config.toml`，若已确认实际账号 CPU 会话并发限制则填写；未知时省略该参数并在运行状态显示未知。调度同时核对其他项目活动 Notebook，不取消其他项目。

将当前账号 Kaggle 凭据放入 `/etc/nba-orderbook/credentials.env`，权限 0600；格式为 `KAGGLE_API_TOKEN=...`。不要将凭据写进源码、Notebook 或日志。Notebook 使用 Kaggle 注入的运行身份发布，必须先通过私有 QA Dataset 权限验证。

先运行 `nba-orderbook plan` 检查未来赛程和峰值并发，然后运行一次 `nba-orderbook --config /etc/nba-orderbook/config.toml scheduler` 检查配置。确认后执行：

```bash
sudo systemctl enable --now nba-orderbook.timer
systemctl list-timers 'nba-orderbook*'
journalctl -u nba-orderbook.service
nba-orderbook --config /etc/nba-orderbook/config.toml status
```

停止新发现与提交：`sudo systemctl disable --now nba-orderbook.timer`，并逐个停止已列出的一次性 `nba-orderbook-<批次>.timer`。这不会终止 Kaggle 上的活动采集。

扫描固定每小时 :00、:30；一次性任务只提交对应批次。状态不明的提交会先查询远端，不启动第二写入者。过期窗口登记错过，不历史补采。运行状态保存在 `/var/lib/nba-orderbook/state.json`。

每场停止后上传完整的 `orderbook.jsonl.gz`、`metadata.json`、`summary.json`。Kaggle 展开 gzip，最终 Dataset 的三个文件为 `orderbook.jsonl`、`metadata.json`、`summary.json`。发布核对确认 Dataset ready，并以远端展开后的三个文件名检查可见性。目录仅在内容变化时创建新版本。不对已发布数据全量下载或重哈希。首次真实验收须确认双方开赛前一分钟已有快照、各场独立结束发布、下一轮目录可检索，并复核原有服务状态。

CPU 批次会话公开并发上限为 5，模板据此配置，并可按账号实际限制覆盖。[Kaggle 工作人员说明](https://www.kaggle.com/discussions/product-feedback/483684)与[近期实际返回的上限错误](https://www.kaggle.com/discussions/product-feedback/718954)支持该值。当前活动查询覆盖各 Notebook 最新版和本服务登记的准确版本，不能证明没有其他手动旧版本会话。
