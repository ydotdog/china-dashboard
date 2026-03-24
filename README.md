# China Dashboard

中国关键指标观测台 — China Structural Indicators Dashboard。

## 功能

追踪中国经济结构性指标，通过信号灯系统直观展示各指标的健康状态：

- **居民消费占 GDP 比重** — 内需驱动程度
- **政府卫生支出占比** — 社会保障水平
- **OECD 综合领先指标** — 经济周期位置
- **青年失业率** — 劳动力市场压力

## 数据源

- World Bank API
- FRED（ILO 数据）

## 技术栈

- HTML / JavaScript（前端看板）
- Python（`scripts/fetch_all_data.py` 数据拉取）
- Python（`server.py` 本地服务）

## 使用方式

双击 `start.command` 即可启动本地服务并打开看板。
