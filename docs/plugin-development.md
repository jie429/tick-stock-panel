# 数据源插件开发指南

数据源插件是可选的行情数据来源(fuyao、麦蕊智数、stock-sdk、akshare 等),作为独立模块放在
`backend/app/plugins/` 下。services 层(kline_sync / quote_service / financial_sync)
全部通过统一路由点分流:插件声明了某数据集就走插件,未声明自动回退 TickFlow。
因此**日K、分钟K、实时、除权和财务等已接通的数据集**只需正确实现契约，无需改动
service / API 代码；`depth5` 是内置 Provider 专属的盘口契约，新增该数据集时还需
确认五档服务已具备对应路由与测试。
反过来,插件也必须遵守内部数据契约(单位、代码格式、复权口径),框架不会替你转换。

> 无代码接入(纯 HTTP YAML 配置)请看 [custom-data-source.md](./custom-data-source.md),
> 两种方式遵循同一套内部数据契约。

## 快速上手

一个插件 = 一个目录 + 一个 `plugin.yaml` 清单:

```
backend/app/plugins/<your_plugin>/
├── plugin.yaml          # 清单(必需)
├── provider.py          # Provider 实现(必需)
├── ...                  # client/桥接/依赖文件(按需)
```

### plugin.yaml 字段

```yaml
name: my_source                          # 唯一标识, 只允许 [a-z0-9_], 也是 provider name
display_name: "我的数据源"                 # 设置页显示名
runtime: none                            # 运行时类型: node | python | none
entry: app.plugins.my_source.provider:MyProvider   # provider 类的导入路径
check: app.plugins.my_source.bridge:availability   # 可用性检测函数(可选)
datasets: [realtime]                     # 支持: daily/adj_factor/minute/full_minute/realtime/depth5/financial
api_key_env: MY_SOURCE_API_KEY           # (可选)声明后设置页提供 Key 输入框
hidden: false                            # (可选)true = 已加载但对设置页隐藏,不注册不展示
description: "数据源描述"
install_hint: "pip install xxx"          # 未装依赖时显示的安装提示
homepage: "https://example.com"          # (可选)官网/申请地址, 显示在设置页 Key 配置说明中
```

只声明真实提供的数据集;未声明的数据集 `provider_has_dataset` 返回 False,自动回退
TickFlow。不要声明做不了的数据集(粒度含义见下文"能力声明的粒度")。

#### api_key_env(界面配置 API Key)

声明 `api_key_env` 的插件可以在设置页的数据源卡片中直接填写 Key, 对齐
TickFlow 的「先探后存」语义:

1. entry 模块需提供模块级 `probe_api_key(key) -> (ok, reason)`,
   后端用候选 Key 实探一次, **无效不落盘**
2. 有效则写入 `data/user_data/secrets.json` 的 `{name}_api_key` 字段
   (0600 权限, 优先级高于 `.env` / 环境变量)
3. 保存后自动重载数据源注册表, 插件即刻变为可切换
4. 插件取 Key 用 `secrets_store.get_env_backed_secret("{name}_api_key", api_key_env)`,
   保证 secrets.json 与 .env 两条配置路径一致

### runtime 字段说明

| runtime | 含义 | 典型场景 |
|---|---|---|
| `python` | 纯 Python 依赖, `pip install` | akshare、tushare |
| `node` | 需要 Node.js 运行时, `npm install` | stock-sdk |
| `none` | 无额外依赖 | 纯 HTTP API 源 |

> ⚠️ stock-sdk 在 Docker 中默认不打包(合规考虑:它抓取第三方财经网站接口,存在版权与
> 反爬风险)。如需启用,构建时传 `--build-arg INCLUDE_STOCKSDK=1`,使用风险自负。
> 详见 [deployment.md](./deployment.md)。

`runtime` 字段当前仅用于 UI 展示, 实际依赖检测由 `check` 函数负责。

### check 函数

插件自己负责检测依赖/Key 是否就绪。后端启动时会调用此函数:

```python
# app/plugins/my_source/provider.py (或 bridge.py)
def availability() -> tuple[bool, str]:
    """返回 (是否可用, 原因)。不抛异常。"""
    if not get_api_key():
        return False, "未配置 MY_SOURCE_API_KEY(可在设置页数据源卡片中直接填写)"
    return True, "ok"
```

- **可用** → 插件注册进路由表, 设置页可切换
- **不可用** → 设置页显示插件卡片但灰显, 展示原因/`install_hint`

## 内部数据契约(所有数据集必须遵守)

以下口径是全项目红线(详见 CONTRIBUTING §3)。金融数据错误往往不抛异常,而是生成
**看似合理的错误结果**——单位、代码格式、复权口径错了,页面照样能渲染,只是数字全错。
插件必须在 provider 内完成适配。

### 代码格式

- symbol 统一带交易所后缀: `600519.SH` / `000001.SZ` / `300750.SZ`; ETF、指数同格式。
- 接口返回裸代码(如 `600519`)或异构格式时,在 client 层实测一页并归一,不要直接透传。

### 单位制

| 字段 | 契约 | 说明 |
| --- | --- | --- |
| `change_pct` | **小数制**, `0.0366` = 3.66% | 接口给百分数(3.66)时必须在 provider 内显式 /100 |
| `turnover_rate`(realtime 入口) | **小数制**, `0.05` = 5% | 下游 enriched 管道统一转百分数值存储 |
| `volume` | **手**, `436231` = 43,623,100 股 | 日K 与实时快照均以手计(1手=100股), 股票/ETF/指数一致(与上游 TickFlow 口径一致, 可用 amount÷volume÷100≈当日均价自验); 接口给股时必须在 provider 内显式 /100(参考 fuyao) |
| `amount` / `turnover` | 元 | |
| 日K OHLC | **不复权原始价** | 复权由 adj_factor + enriched 管道处理, provider 不得自行复权 |

### 缺字段与空数据

- 接口不提供的字段返回 `None`,禁止"数值小于 1 就乘 100"之类启发式补全——那会掩盖
  真实的数据错误。
- 可推导字段按固定口径推导: `change_pct = change_amount / prev_close`(小数制,不乘 100)。
- 接口结构整体变化(如所有行都识别不出 symbol)要打明确告警日志,不要静默返回空数据。

## 能力声明的粒度(重要)

`datasets` 声明是**数据集级**的,不是资产类型级的:声明了 `realtime`,整个全市场实时
轮询周期(含指数与 ETF 部分)就全部路由给插件。若你的快照只覆盖 A 股股票:

- 指数行情自动降级为日线推导值(非实时),不报错;
- ETF 实时计数为 0。

这是当前框架的设计行为。要么在数据里尽量覆盖指数/ETF,要么接受降级并在
`description` 里向用户说明覆盖范围。

## Provider 接口契约

Provider 是普通 Python 类(无需继承基类),方法签名对齐 `GenericHTTPProvider`,
services 层零改动即可路由。只实现已声明数据集对应的方法,其余可缺省。

```python
class MyProvider:
    name = "my_source"
    builtin = True  # 标记为内置(不可被用户编辑/删除)
    # 可选: 未声明时只覆盖 stock 日K/维表; 自定义源可显式扩展至 index/etf。
    daily_asset_types = frozenset({"stock"})
    instrument_asset_types = frozenset({"stock"})
    # 仅在 Provider 真正支持全市场分钟落盘时才能设为 True。
    supports_minute_universe_sync = False
    # 可选: 分钟K历史窗口(交易日); 未声明表示不主动收窄。
    minute_history_days = 5
    # 可选: False 表示请求或格式校验失败时保持来源隔离, 不切换 TickFlow。
    fallback_to_tickflow_on_error = False

    def __init__(self):
        self.config = MyConfig()  # 需有 .datasets 属性(dict, key 是数据集名)

    def close(self) -> None:
        """清理资源(load_all 重建注册表时会调)。"""

    def get_daily(self, symbols, start_time, end_time, asset_type="stock",
                  on_chunk_done=None) -> pl.DataFrame:
        """日K: [symbol, date, open, high, low, close, volume, amount]; 不复权"""

    def iter_daily(self, symbols, start_time, end_time, asset_type="stock",
                   on_chunk_done=None) -> Iterator[pl.DataFrame]:
        """(可选)有界分批返回与 get_daily 同形的日K; 全市场历史同步优先消费。"""

    def get_adj_factors(self, symbols, start_time, end_time, asset_type="stock",
                        on_chunk_done=None) -> pl.DataFrame:
        """除权因子: [symbol, trade_date, ex_factor]"""

    def get_minute(self, symbols, start_time, end_time, asset_type="stock",
                   on_chunk_done=None, freq="1m") -> pl.DataFrame:
        """分钟K: [symbol, datetime(北京墙钟), open, high, low, close, volume, amount(元, 可空)]"""

    def get_intraday_batch(self, symbols, count=300, asset_type="stock") -> pl.DataFrame:
        """(声明 full_minute 数据集时实现) 全量分钟修复轮: 给定标的当日 1 分钟K,
        canonical 8 列同 get_minute; 内部自行分块/限速。"""

    def get_intraday_latest(self, symbols=None, count=3) -> pl.DataFrame:
        """(可选, full_minute 稳态增量轮) 尽量单请求返回全市场每只最新 count 根;
        未实现则服务降级为仅修复轮 (节奏下限 60s)。"""

    def get_realtime(self) -> list[dict]:
        """全市场实时快照 → list[dict]。失败软返回 [], 不抛异常(不阻断轮询线程)。"""

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """(可选)指数实时快照 → list[dict], 行字段与 get_realtime 一致。
        A 股快照普遍不含指数(fuyao 的指数在独立端点); 声明 realtime 的源
        强烈建议实现本方法, 否则指数行情冻结在本地日K兜底。失败返回 None,
        成功但无数据返回 []。"""

    def get_market_auction_snapshot(self) -> list[dict] | None:
        """(可选, 全市场竞价扫描) A 股 09:25 集合竞价终态快照 → list[dict], 行字段
        见下文「竞价快照行字段」。失败返回 None (调用方保留上轮落盘结果),
        成功但当日无竞价成交返回 []。"""

    def get_board_groups(self, kind: str = "concept") -> list[dict] | None:
        """(可选, 板块分组成分) 概念板块成分 → list[dict], 行字段见下文「板块分组行字段」。
        只定义 kind="concept"; 其它取值返回 None(明确不支持, 不给"成功但空"的假结果),
        软失败同样返回 None。逐板块取成分属显式触发的批量拉取, 不要放进实时轮询路径。"""

    def get_depth5(self, symbols) -> dict:
        """仅内置 Provider：{symbol: {ask_volumes, bid_volumes, timestamp(ms)}}，
        可选带 ask_prices / bid_prices(元, 与量同源同档)。
        缺档用 None，不得把缺失伪造成 0；调用失败必须返回 {}，不得切换 TickFlow。"""

    def get_trade_flow(self, symbols) -> dict:
        """(可选, 成交方向) {symbol: {inside_volume, outside_volume}}。
        内盘 = 当日主动卖出量、外盘 = 当日主动买入量，单位「手」。
        未实现即视为本源不提供成交方向；调用失败返回 {}，不换源、不伪造成 0。"""

    def get_financials(self, table, symbols, latest_only=False) -> pl.DataFrame:
        """财务数据(声明 financial 数据集时实现, table 见 financial_sync 调用)。"""

    def get_instruments(self, asset_type="stock") -> list[dict]:
        """(可选)标的维表: 返回 tickflow Instrument 形状的行, 供 instrument_sync 复用 flatten"""

    def test_dataset(self, dataset: str, symbols=None) -> dict:
        """(强烈建议)设置页"试拉"按钮。
        返回 {provider, dataset, rows, columns, preview, error?}; 未支持的数据集
        返回 error 字段说明会回退 TickFlow。"""
```

`get_depth5` 仅供内置 Provider 插件声明。它返回以 symbol 为键的标准盘口字典；数量数组按
一档到五档排列(下标 0 = 买一 / 卖一)，单位为“手”，`timestamp` 为毫秒 Unix 时间戳。
`ask_prices` / `bid_prices` 是可选扩展：同档位的价格(元)，价与量必须来自同一次盘口快照，
补不齐的档位两侧一起保留 `None`，不得用 `0` 或昨收凑数。服务层负责分片限速，provider
不应自行切换或回退到其他数据源；YAML HTTP 自定义源不能声明 `depth5`。

```python
{
    "600519.SH": {
        "bid_volumes": [10, 20, 30, 40, 50],
        "ask_volumes": [12, 22, 32, 42, 52],
        "bid_prices": [1680.00, 1679.99, None, None, None],
        "ask_prices": [1680.02, 1680.05, None, None, None],
        "timestamp": 1788505200000,
    },
}
```

### get_minute 的 datetime 时区契约

`datetime` 必须是**北京时间墙钟**（naive，如 `2026-08-28 09:35:00`），与日K的
`date` 语义对齐；不要返回 UTC 或带时区的时间。前端分时图按交易时段时轴
（09:30–11:30 / 13:00–15:00）映射每根K线，UTC 口径的帧会导致全部点位落在时轴外、
分时图空白。

`amount` 单位为元；数据源无法提供可靠的分钟成交额时应返回 `null`，不得伪造。
成交额缺失后无法继续计算累计成交均价，前端会停止绘制后续均价线并显示 `—`。

入口守卫（`kline_sync._enforce_minute_beijing_wallclock`）对所有分钟源强制归一：
带时区 → 自动换算成北京墙钟；naive 但整体呈 UTC 特征（如 01:30）→ 自动 +8 纠偏并
记日志；完全无法识别的口径 → 拒收并回退 TickFlow。契约仍要求源头写对，守卫只是兜底。

可选类属性 `minute_history_days = 5` 声明 1 分钟历史深度（交易日）；未声明视为
深历史（TickFlow 基准）。浅源（如 stock-sdk 免费分时仅保留最近 5 个交易日）声明后，
个股分时档位自动收窄为可行选项并默认 5 日，深源默认 20 日。

> **全量分钟 (full_minute) 数据集契约**:声明 `full_minute` 数据集并把
> `full_minute_data_provider` 路由到你的源,即接入「全量分钟」能力(盘中全市场
> 当日分钟K增量落盘,由内置服务 `minute_refresh` 调度,与 TickFlow Expert 同一
> 能力键 `intraday.universe`)。需实现:
>
> - `get_intraday_batch(symbols, count=300, asset_type="stock") -> pl.DataFrame`
>   — **必须**(或已有 `get_minute` 自动回退,但强烈建议实现批量端点)。
>   返回给定标的当日 1 分钟K,canonical 8 列
>   `[symbol, datetime(北京墙钟 naive), open, high, low, close, volume, amount]`,
>   内部自行分块/限速。服务在冷启动、覆盖断档、连续空轮时调用(修复轮)。
> - `get_intraday_latest(symbols=None, count=3) -> pl.DataFrame` — **可选**,
>   稳态增量轮专用:尽量单请求返回全市场每只标的最新 `count` 根。未实现时
>   服务自动降级为仅修复轮,节奏下限抬到 60s(全天批量打不住 6s 节奏)。
>
> 两个方法的返回帧都过 `_enforce_minute_beijing_wallclock` 时区守卫(与
> `get_minute` 同纪律);失败抛异常或返回空 df 均按空轮处理,连续空轮触发
> 修复轮自愈。声明方式:插件在 `plugin.yaml` 的 `datasets:` 列表加入
> `full_minute`。YAML 声明式源同样支持(数据集配置与 `minute` 同形,仅提供
> 修复轮语义,见 [custom-data-source.md](./custom-data-source.md))。

可选类属性 `daily_asset_types` 与 `instrument_asset_types` 分别声明日K和基础标的维表
覆盖的资产类型。未声明时为兼容旧 Provider, 均按仅 `stock` 处理。`get_instruments()`
仅应返回上游可可靠给出的字段; 如只有代码表, 不得伪造股本、涨跌停等金融元数据。

`supports_minute_universe_sync` 默认应为 `False`。能按标的或分组拉取分钟K, 不等于能
承受全市场分钟落盘；未明确声明时，全市场入口会拒绝该 Provider。若分钟源必须保持
来源隔离，可设置 `fallback_to_tickflow_on_error = False`；请求失败或分钟时间契约校验
失败时会返回空结果而不会隐式调用 TickFlow。

### 异常语义

| 方法 | 失败行为 |
| --- | --- |
| `get_realtime` | **软失败**: 返回 `[]` + warning 日志, 保证轮询线程不中断 |
| `get_realtime_indices` | **软失败**: 返回 `None` + warning 日志, 保留上轮有效缓存; 成功无数据返回 `[]` |
| `get_market_auction_snapshot` | **软失败**: 返回 `None` + warning 日志, 服务回退到上一份落盘快照; 成功但全市场无竞价成交返回 `[]` |
| `get_board_groups` | **软失败**: 返回 `None` + warning 日志, 调用方保留已有归属数据; 不支持的 `kind` 同样返回 `None`, 不降级成别的分类 |
| `get_depth5` | 单批异常由服务隔离；不跨数据源回退；可选价格缺失/非法只丢价格, 不影响封单判定 |
| `get_trade_flow` | **软失败**: 未实现或调用异常只丢成交方向, 五档不受影响; 不换源、不伪造成 0 |
| `get_minute` | 默认抛异常时调用方回退 TickFlow；设 `fallback_to_tickflow_on_error = False` 时 fail-closed |
| `get_daily` / `get_adj_factors` / `get_financials` | 异常由上层同步流程捕获记录; 无数据返回空 DataFrame |
| `iter_daily` | 可选; 每批必须符合 `get_daily` 契约。流正常结束后才提交 staging; 未捕获异常会丢弃 staging。provider 内已定义的单标的软失败语义保持不变 |

`iter_daily` 用于避免大范围日K同步在 provider 内累积完整 DataFrame。实现该方法后,
`kline_sync` 会优先消费它; 未实现的 provider 继续调用 `get_daily`,保持兼容。批次大小应有
明确上界,不得先把全部结果放入列表再 `concat`。`on_chunk_done(cur, total)` 必须覆盖空批次,
确保最终 `cur == total`。

### get_realtime 行字段

| 字段 | 必需 | 契约 |
| --- | --- | --- |
| `symbol` | ✅ | 标准代码带后缀 |
| `last_price` | ✅ | 最新价 |
| `prev_close` | ✅ | 昨收, 涨跌幅推导基准 |
| `open` / `high` / `low` | ✅ | 当日 OHLC |
| `volume` | ✅ | **手**(1手=100股) |
| `amount` | 建议 | 成交额(元) |
| `change_pct` | 建议 | **小数制**; 缺失时下游按 change_amount/prev_close 推导 |
| `change_amount` | 建议 | 涨跌额(元) |
| `timestamp` | 建议 | 毫秒; 优先用服务端时间(行情归属), 缺失退本地时间 |
| `name` | 可选 | 快照无名称时置 None, 下游用标的维表关联 |
| `amplitude` / `turnover_rate` / `session` | 可选 | 缺失置 None, 不启发式伪造; turnover_rate 入口为小数制 |

### get_market_auction_snapshot 行字段

全市场竞价扫描的每行 = 一只标的的 09:25 集合竞价终态。单位口径与其它数据集一致:
价格为元、量为**手**、金额为元; 涨跌幅为**小数制**(上游同类字段若是百分数, 必须自行
推导)。停牌/无竞价成交(今开或竞价额为 0)的标的不返回, 不得伪造成 0 参与竞价量比。

| 字段 | 必需 | 契约 |
| --- | --- | --- |
| `symbol` | ✅ | 标准代码带后缀 |
| `open_price` | ✅ | 今开 = 竞价成交价(元) |
| `prev_close` | ✅ | 昨收/除权参考价(元), 涨跌幅推导基准 |
| `auction_amount` | ✅ | 竞价成交额(元) |
| `auction_volume` | ✅ | 竞价成交量(**手**); 源只给竞价额时按 `竞价额 ÷ 今开 ÷ 100` 还原 |
| `open_pct` | 建议 | 开盘涨幅(小数制) |
| `change_pct` / `last_price` | 建议 | 现价涨跌幅(小数制) / 最新价 |
| `bid1` / `ask1` / `bid_volume1` / `ask_volume1` | 建议 | 一档盘口(价 元, 量 手); 缺失置 `None` |
| `seal_amount` | 建议 | 封单额(元) = 买一价 × 买一量(手) × 100 |
| `inside_volume` / `outside_volume` | 建议 | 当日主动卖/买量(手), 两者之和 ≈ 当日成交量 |
| `amount` / `volume` | 可选 | 当日累计成交额(元) / 成交量(手) |
| `timestamp` | 建议 | 毫秒; 无服务端时间时用本轮拉取时间, 不伪造历史时间戳 |

服务层 (`app/services/auction_scan.py`) 负责按日落盘、竞价量比与筛选, 插件只做单位
归一: 竞价量比 = 今日竞价量 ÷ 上一可得快照日竞价量, 首日无基线时不筛量比。

### get_board_groups 行字段

板块分组成分(可选协议)按标的返回一行, 供扩展数据预设等上层折叠成「所属概念 / 所属行业」
这类维度字段。`kind` 是分类标识, 目前只定义 `"concept"`(通达信概念板块, 实测 269 个
板块 / 约 5 万条归属)。不支持的取值返回 `None`, 不用空数组冒充"成功但空"。

| 字段 | 必需 | 契约 |
| --- | --- | --- |
| `symbol` | ✅ | 标准代码带后缀(上游给 `sh600000` 这类代码时要自行归一) |
| `groups` | ✅ | 该标的所属板块名列表; 无归属的标的不返回, 不返回 `groups` 为空的残行 |

服务层 (`app/services/ext_presets.py`) 把 `groups` 用 `;` 拼成「所属概念」列, 与同花顺概念
预设同 schema, 因此消费方(概念分析 / RPS 轮动 / 信号筛选)不区分来源。

### 五档盘口与成交方向

`get_depth5` 的每行 = 一只标的一次盘口快照, 连板梯队封单判定与个股详情弹窗的盘口卡片
共用同一条能力路由与限速(缓存与 SSE 纪律见 CONTRIBUTING)。`get_trade_flow` 是**可选协议**,
只被盘口卡片消费: 未实现时前端隐藏成交方向那一段, 五档照常展示。

| 字段 | 必需 | 契约 |
| --- | --- | --- |
| `bid_volumes` / `ask_volumes` | ✅ | 买一→买五 / 卖一→卖五挂单量(**手**), 长度 5, 缺档 `None` |
| `bid_prices` / `ask_prices` | 可选 | 同档位价格(元); 与量同源同档, 缺档 `None`; 整段缺失/非法只丢价格 |
| `timestamp` | ✅ | 毫秒; 缺失则整行丢弃 —— 展示层必须能说明这是哪一刻的盘口 |
| `inside_volume` | 可选 | 内盘 = 当日主动卖出量(**手**), 来自 `get_trade_flow` |
| `outside_volume` | 可选 | 外盘 = 当日主动买入量(**手**); 两侧都缺则整只不返回 |

成交方向是**全日累计**的主动买卖量(与逐笔成交方向不同), 两者之和 ≈ 当日成交量, 与全市场
竞价扫描的同名字段同一口径; 单侧缺失保留 `None` 而不补 0。展示侧的口径: 盘口量单位是
「手」, 只有封单额才 ×100; 缺档显示 `—`。

### config.datasets 的作用

`provider_has_dataset(name, dataset)` 通过 `dataset in provider.config.datasets` 判断。
这是 services 层路由的关键: 用户在设置页选了插件, 但某数据集未声明时, 该数据集
自动回退 TickFlow。

```python
class MyConfig:
    datasets = {"daily": ..., "realtime": ...}  # key 是数据集名, value 任意
```

## 限频与性能

- realtime 默认 6s 轮询一轮。优先确认服务端单次 limit 上限: fuyao 实测单页
  limit=6000 可一次拉完全市场(~5600 只), 1 请求/轮; 若服务端强制小页, 必须做
  页间隔/自限速(参考 fuyao 的 0.15s 页间隔兜底), 并建议用户把轮询间隔调大(15-30s)。
- 分页必须有页数上限(防 count 异常导致死循环)和空页终止条件。
- 拉取由 fetch 锁串行化, 慢不会并发重叠; 实际刷新周期 = 轮询间隔 + 拉取耗时,
  串行分页的全量快照本身就需要数秒, 不要按"6s 内必须完成"设计。

## 测试要求

插件 PR 必须带契约测试(CONTRIBUTING §9), **不依赖真实网络与 API Key**——用假
Client/桥接注入。以 `backend/tests/test_fuyao_provider.py` 为范本, 至少覆盖:

1. 字段映射与单位转换: 百分数→小数制、volume 股→手（已为手则原样）、*ms 零点戳时区换算、缺失字段按口径推导、缺失字段置 None 不伪造
2. 接口响应结构变体: 实测结构 vs 官方文档示例双兼容(供应商文档与实际不一致是常态)
3. 分页: 多页合并、空页终止、页数上限
4. 软失败: 接口报错返回 []; 整页 schema 变化有告警而非静默空数据
5. 能力声明: 未声明数据集 `provider_has_dataset` 为 False
6. Key 语义: 先探后存(无效不落盘)、secrets.json > .env 优先级、availability 两态
7. loader 集成: 清单解析后正确注册(或 hidden 时正确跳过)

```bash
cd backend && uv run --extra dev python -m pytest tests/test_<your_plugin>_provider.py -q
uv run --extra dev python -m ruff check app/plugins/<your_plugin>/ tests/test_<your_plugin>_provider.py
```

## 现有插件参考

- **`backend/app/plugins/fuyao/`** — 同花顺官方 REST 数据源(runtime: none, 纯 HTTP 零依赖)
  - 提供 `realtime`(A 股全市场快照, 分页拉取)、`daily`(原始价日K三档: 近端窗口走 daily-k-10d dump, 深窗口走 daily-k 10 年全量 dump(172MB 一次下载、缓存复用、10d 补尾), 兜底单标的接口按 10 年自动分片)、`adj_factor`(事件 dump + 前收盘价从本地日K dump 一次取齐、缺价标的回退单标的接口, 按交易所公式推导单事件比值, 涨跌停自检; 全市场配价从逐标的 ~13 分钟降为秒级); Key 在设置页卡片直接配置(先探后存), 或 `.env` 配 `FUYAO_API_KEY`
  - `client.py` — httpx 客户端(X-api-key 认证 + 统一信封解包 + 分页 + 页间隔限频 + 单标的日K + dump 预签名下载, S3 下载不带 Key 头)
  - `provider.py` — Provider 实现(实测/文档双字段名映射、百分数→小数制、volume 股→手、上海零点戳 +8h 时区、dump 按 release 版本缓存、软失败、Key 探测)
  - `tests/test_fuyao_provider.py` — 73 个契约测试, 是新插件的测试范本
- **`backend/app/plugins/mairui/`** — 麦蕊智数沪深 A 股 REST 数据源(runtime: none, 纯 HTTP 零依赖)
  - 提供 `daily`、`realtime`、`depth5`、`financial`,其中实时行情按官方每批 20 只与基础限频分批拉取,Provider 将轮询下限抬至 60 秒
  - 日K与实时 `volume` 上游原生为手,不重复换算;`change_pct`、`amplitude`、`turnover_rate` 从百分数显式 `/100`;五档缺档保留 `None`
  - 财务三表、主要指标与公司股本统一映射到 `period_end` / `announce_date` 和项目 canonical 字段,供应商独有数值列同时透传
  - 普通 licence 无 1 分钟能力,不声明 `minute/full_minute`;近年分红接口不覆盖完整配股事件,不声明 `adj_factor`
  - licence 在设置页先探后存,或通过 `MAIRUI_LICENSE` 配置;真实 licence 永不写入代码、清单或测试
- **`backend/app/plugins/eltdx/`** — eltdx 免费通达信行情协议数据源(runtime: python, 直接复用 [eltdx](https://github.com/electkismet/eltdx) 的 TCP 客户端, 不启动它的 MCP/HTTP 服务, 无 API Key)
  - 依赖 `eltdx>=3.2,<4`: 桌面发行版由 `desktop` extra 内置(`packaging/tickflow.spec` 里 `collect_all("eltdx")`), 源码/容器安装由插件目录的 `requirements.txt` 经设置页安装。3.0 起上游 API 按命名空间重写, 0.5.x 的扁平接口已全部移除, 不能沿用旧代码
  - `bridge.py` — 轻量边界: `availability()` 只做导入 + 版本区间 + 实例方法存在性检查(**不探测 TCP 主机**, 插件扫描不能因测速阻塞启动); `create_client()` 不接受任何外部 host, 只用 eltdx 自带默认行情服务器列表, 避免可选源变成任意 TCP 出口
  - 提供 `daily`(股票/ETF/指数不复权原始日K)、`adj_factor`、`minute`(按标的 1 分钟 OHLCV, 实测历史约 100 个交易日)、`realtime`(A 股 + ETF 全市场快照)、`depth5`(五档价 + 量), 并有可选协议 `get_trade_flow`(内盘/外盘)
  - 单位口径: 价格与成交额为元, `volume_lots`/`total_hand` 原生为**手**, 原样透传不再换算; `change_pct`/`amplitude` 统一由 `change_amount/prev_close` 推导成小数制, 不混用上游的百分数属性
  - 指数成交量差 100 倍: 实测 `000001.SH`/`399001.SZ`/`399006.SZ`/`000016.SH` 的日K `volume_lots` 恰为当日快照 `total_hand` 的 1/100(当日成分股快照之和与指数快照一致), 故指数 K 线成交量 ×100 还原为手; 股票/ETF 无此偏差
  - 报价协议单请求上限 80 只(超出静默截断, 整批上千只直接断流): `realtime`、指数补拉与 `depth5` 都自行按 80 只切批; 全市场 A 股 + ETF 约 7200 条实测 ~6 秒, 故 `realtime_min_interval = 30`
  - 全市场竞价扫描走**分类行情** `quotes.list_by_category("沪深A股", sort_by="开盘金额")`(0x054b): 单页 80 条、全市场 5575 只 / 70 页实测 ~4 秒, 一条记录同带竞价成交额、今开、昨收、一档盘口、内盘/外盘与封单额, 不必按标的逐只拉竞价明细。竞价成交量由 `竞价额 ÷ 今开 ÷ 100` 还原为手(集合竞价全部以开盘价成交, 实测与上游 09:25 撮合量一致); 上游 `change_pct` 属性是百分数, 与其它数据集一样自行推导小数制。停牌/无竞价成交(今开或竞价额为 0)的行不进入结果, 不伪造成 0 参与竞价量比。软失败返回 `None`(保留上轮), 页数上限 200 作死循环兜底
  - 概念板块分组走可选协议 `get_board_groups("concept")`: 板块定义取 `infoharbor_block.dat` 的 GN_ 段(实测 269 个板块), 成分要逐板块取(实测 40 个板块 ≈ 5.7 秒 → 全量约 40 秒 / 约 5 万条归属), 定义文件按日缓存在数据目录 `cache/eltdx_boards/`。只开放概念: 行业板块的定义文件(`tdxzs.cfg` / `tdxzs3.cfg`)不在服务器可下载列表内(只随本机通达信客户端分发), 请求其它 `kind` 一律返回 `None` 而不是猜; 板块名会跨分类重名(如 `通达信88` 在概念与风格里各有一个定义), 所以不做跨分类合并
  - 五档必须用 `helpers.full_quotes`(快照 + 0x0547 刷新流合并), `quotes.get_snapshots` 只给一档; 补齐后价(`QuoteLevel.price`)与量(`QuoteLevel.volume`)一起取, 补不齐的档位两侧都保留 `None`, 绝不伪造成 0
  - 成交方向走可选协议 `get_trade_flow`: 快照自带的 `inside_dish`(内盘 = 主动卖出量)与 `outer_disc`(外盘 = 主动买入量)原样透传, 单位已是手; 只被个股盘口卡片消费(全市场竞价扫描走 `get_market_auction_snapshot`, 两者同字段口径: 全日累计, 不是逐笔方向)
  - 指数不混入全市场快照: 通达信"指数"代码表含约 3000 只板块/题材指数, `realtime` 只收 A 股 + ETF, 指数走可选协议 `get_realtime_indices` 按需单拉(失败返回 `None`, 让上层保留上轮指数缓存)
  - K 线分页: `bars.get` 的 `start` 是**相对最新一根的偏移量**且单页上限 800 根, 按窗口起点逐页向前回溯, 带页数上限(82 页 ≈ 1990 年至今日K)与"本页时间不可解析即停止"的兜底
  - `adj_factor` 推导: 取 `corporate.capital_changes` 的除权除息事件(每 10 股口径 c1=现金分红 c2=配股价 c3=送转股 c4=配股)与事件日前的原始日K收盘价, 按 `参考价 = (前收盘×10 − 现金分红 + 配股×配股价) / (10 + 送转股 + 配股)` 得**单事件**比值; 不使用上游逐日前/后复权仿射系数, 那与"单事件因子 + 管道自行累积"的契约不同构
  - 不声明 `financial`(上游只有简版财务批量字段)与 `full_minute`(按标的拉取撑不住盘中全市场分钟落盘); `fallback_to_tickflow_on_error: false` 来源隔离, 故障时返回明确空结果
  - `tests/test_eltdx_provider.py` — 121 个契约测试(单位换算与指数成交量口径、分页方向与页数上限、80 只切批、五档价与缺档、成交方向的内外盘映射与 80 只切批/软失败、全市场竞价扫描的归一与翻页/页数上限/去重/软失败、概念板块分组的折叠与软失败三态、除权因子公式、能力声明、availability 两态、loader 注册); `tests/test_eltdx_desktop_packaging.py` — 桌面打包静态契约; `tests/test_auction_scan.py` — 竞价扫描服务契约(状态机、落盘与 TTL 缓存、竞价量比与阈值、名称补全, 全离线)
- **`backend/app/plugins/stocksdk/`** — Node 型插件, 通过 subprocess 桥接调用 stock-sdk
  - `bridge.py` — Python↔Node 桥接 + availability 检测
  - `bridge.mjs` — Node 端(并发池、重试、SDK 解析)
  - `provider.py` — Provider 实现(归一化、分批、错误降级)

## 路由机制(无需关心, 仅参考)

后端启动时, `loader.py` 扫描 `plugins/` 目录:
1. 读每个子目录的 `plugin.yaml`
2. `hidden: true` → 跳过(不注册不展示); 否则调 `check` 函数检测可用性
3. 可用 → 动态 import `entry` 指向的 Provider 类 → 注册进 `_PROVIDERS`
4. 不可用 → 记录状态, 设置页显示但不可切换

注册后, 插件和用户 YAML 自定义源走**完全相同的路由路径**(services 层的
`provider_has_dataset` / `get_provider` 调用), 无需额外集成代码。
