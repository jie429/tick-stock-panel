/**
 * 个股盘口卡片 — 五档买卖报价 (价 + 量) 与成交方向 (内盘/外盘)。
 *
 * 数据来自 GET /api/quote/book: 与连板梯队封单走同一条 depth5 能力路由与限速,
 * 单只按需拉取, 不进盘中轮询热路径。
 *
 * 挂载位置: 只在个股详情弹窗的**分时视图**里 (当日视角), 日K视图不显示 —— 盘口与
 * 成交方向都是当日实时/当日累计数据, 与历史日K不是同一时间尺度。
 *
 * 口径与纪律:
 * - 盘口量单位是「手」(数据源原样), 只有封单额才 ×100; 价与量同源于一次快照。
 * - 缺档显示「—」而不补 0 —— 伪造的 0 会被读成「卖一挂 0 手 = 真封板」。
 * - 成交方向是可选协议: 数据源没提供时只隐藏该段, 不影响五档。
 *
 * 覆盖五态: 加载中 / 能力不可用(引导去配置数据源) / 读取失败 / 该股暂无盘口 / 有数据。
 * 轮询只在交易时段开启, 休盘不空转 (打开弹窗那一次快照仍然展示)。
 */
import { useQuery } from '@tanstack/react-query'
import { api, type QuoteBook } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { MissingCapChip } from '@/lib/capability-labels'
import { useQuoteStatus } from '@/lib/useSharedQueries'
import { cn } from '@/lib/cn'
import { fmtBigNum, fmtPrice } from '@/lib/format'

/** 交易时段轮询间隔: 五档变化比最新价慢, 15s 足够且省上游配额 */
const BOOK_REFETCH_MS = 15_000

/** 展示顺序: 卖五→卖一, 买一→买五 (与通达信盘口一致); idx 是档位下标 (0 = 一档) */
const LEVELS: { side: 'ask' | 'bid'; label: string; idx: number }[] = [
  { side: 'ask', label: '卖5', idx: 4 },
  { side: 'ask', label: '卖4', idx: 3 },
  { side: 'ask', label: '卖3', idx: 2 },
  { side: 'ask', label: '卖2', idx: 1 },
  { side: 'ask', label: '卖1', idx: 0 },
  { side: 'bid', label: '买1', idx: 0 },
  { side: 'bid', label: '买2', idx: 1 },
  { side: 'bid', label: '买3', idx: 2 },
  { side: 'bid', label: '买4', idx: 3 },
  { side: 'bid', label: '买5', idx: 4 },
]

/** A 股盘口配色: 买红卖绿 (与 红涨绿跌 同源) */
const SIDE_TEXT: Record<'ask' | 'bid', string> = { ask: 'text-bear', bid: 'text-bull' }

function fmtBookTime(ms: number | null | undefined): string {
  if (ms == null) return ''
  const d = new Date(ms)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function LevelCell({ side, label, price, volume }: {
  side: 'ask' | 'bid'
  label: string
  price: number | null | undefined
  volume: number | null | undefined
}) {
  return (
    <div
      className="flex flex-col items-center gap-px rounded bg-elevated/40 px-1 py-0.5"
      title={`${label} ${price == null ? '—' : `${price.toFixed(2)}元`} / ${volume == null ? '—' : `${volume}手`}`}
    >
      <span className="text-[9px] leading-none text-muted">{label}</span>
      <span className={`font-mono text-[11px] leading-tight tabular-nums ${price == null ? 'text-muted' : SIDE_TEXT[side]}`}>
        {fmtPrice(price)}
      </span>
      <span className="font-mono text-[9px] leading-none tabular-nums text-secondary">
        {volume == null ? '—' : fmtBigNum(volume)}
      </span>
    </div>
  )
}

export function QuoteBookPanel({ symbol, className = '' }: { symbol: string; className?: string }) {
  const trading = useQuoteStatus().data?.is_trading_hours ?? false
  const q = useQuery({
    queryKey: QK.quoteBook(symbol),
    queryFn: () => api.quoteBook(symbol),
    enabled: !!symbol,
    staleTime: 5_000,
    // 非交易时段不开轮询: 盘口不再变化, 保留打开时那一次快照即可
    refetchInterval: trading ? BOOK_REFETCH_MS : false,
  })

  const book: QuoteBook | null | undefined = q.data?.book
  const available = q.data?.available
  const inside = book?.inside_volume ?? null
  const outside = book?.outside_volume ?? null
  const flowTotal = (inside ?? 0) + (outside ?? 0)
  // 两侧都为 0 也是真数据 (当日确实没有主动买卖), 只有两侧都缺才隐藏该段
  const hasFlow = book != null && (inside != null || outside != null)
  const buyShare = flowTotal > 0 && outside != null ? outside / flowTotal : null

  return (
    <div className={cn('rounded border border-border/50 bg-elevated/25 px-3 py-1.5', className)}>
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="text-[11px] font-semibold text-foreground">盘口</span>
        <span className="text-[9px] text-muted">五档价 / 量(手)</span>
        {hasFlow && (
          <span className="flex items-center gap-2 text-[10px]">
            <span className="text-muted">
              内盘 <span className="font-mono tabular-nums text-bear">{fmtBigNum(inside)}</span>
            </span>
            <span className="text-muted">
              外盘 <span className="font-mono tabular-nums text-bull">{fmtBigNum(outside)}</span>
            </span>
            {buyShare != null && (
              <span className="text-muted" title="外盘 ÷ (内盘 + 外盘): 主动买占比, 越高越偏买方">
                主动买 <span className="font-mono tabular-nums text-foreground/80">{(buyShare * 100).toFixed(1)}%</span>
              </span>
            )}
          </span>
        )}
        <span className="ml-auto shrink-0 font-mono text-[9px] text-muted" title="盘口快照时间">
          {fmtBookTime(book?.timestamp)}
        </span>
      </div>

      <div className="mt-1">
        {q.isLoading ? (
          <div className="grid grid-cols-5 gap-1 md:grid-cols-10">
            {LEVELS.map(l => (
              <div key={`${l.side}${l.label}`} className="h-[38px] animate-pulse rounded bg-elevated/40" />
            ))}
          </div>
        ) : available === false ? (
          <p className="flex flex-wrap items-center gap-1.5 py-1 text-[10px] text-muted">
            <MissingCapChip capKey="depth5" to={null} />
            <span>当前数据源未提供五档盘口, 可在「设置 → 数据源」把五档盘口指向声明该能力的数据源。</span>
          </p>
        ) : q.isError ? (
          <p className="py-1 text-[10px] text-muted">盘口读取失败, 稍后重试。</p>
        ) : book == null ? (
          <p className="py-1 text-[10px] text-muted">该股暂无盘口 (非交易时段 / 停牌 / 数据源本次未返回)。</p>
        ) : (
          <div className="grid grid-cols-5 gap-1 md:grid-cols-10">
            {LEVELS.map(l => {
              const prices = l.side === 'ask' ? book.ask_prices : book.bid_prices
              const volumes = l.side === 'ask' ? book.ask_volumes : book.bid_volumes
              return (
                <LevelCell
                  key={`${l.side}${l.label}`}
                  side={l.side}
                  label={l.label}
                  price={prices?.[l.idx]}
                  volume={volumes?.[l.idx]}
                />
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
