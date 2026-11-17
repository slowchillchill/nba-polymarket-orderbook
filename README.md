# NBA Polymarket 订单簿

发现 Polymarket NBA 单场比赛的唯一全场胜负市场，采集两个结果资产的全部原始 WebSocket 消息及完整 L2 深度。香港控制服务每半小时发现比赛，按开赛时间两小时跨度分组，以一次性任务在最早 T−90 提交 Kaggle CPU Notebook。

每场独立在明确结算或确认开赛时间加六小时停止，关闭文件后发布一次最终 Dataset。发布在独立线程进行，其他比赛继续接收。总目录为 `qiuqiuslowchillchill/nba-polymarket-orderbook`。

## 使用

需要 Python 3.12。开发使用 `uv sync --locked`，测试使用 `uv run pytest`。

```bash
nba-orderbook discover
nba-orderbook plan
nba-orderbook --config /etc/nba-orderbook/config.toml scheduler
nba-orderbook --config /etc/nba-orderbook/config.toml status
```

`collect --batch batch.json --output /kaggle/working/nba-data` 仅供 Kaggle 执行。批次包含 `id`、`owner` 和比赛数组 `games`。提交后成员固定。

最终 Dataset 每场三个文件：`orderbook.jsonl`、`metadata.json`、`summary.json`。采集端继续顺序写入并上传 `orderbook.jsonl.gz`，Kaggle 展开 gzip 后以 `orderbook.jsonl` 提供原始数据。原始消息记录 UTC 接收时间、连接标识、本地序号及完整原文。`book` 替换整个盘口，`price_change.size` 替换该价格数量，0 删除该档。未知消息仍保留原文。

没有中间发布或检查点。最终发布前 Notebook 异常退出可能丢失此前全部采集。重连快照只能恢复当前盘口，不能补回历史增量。摘要保留实际覆盖、迟到、断线和停止原因，最终版不代表无缺口。

部署和维护见 [部署说明](docs/deployment.md)。本次交付合同和进度见 [Issue #1](https://github.com/slowchillchill/nba-polymarket-orderbook/issues/1)。
