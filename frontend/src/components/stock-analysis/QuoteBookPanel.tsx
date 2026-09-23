/**
 * 个股盘口面板 — 五档买卖报价 (价 + 量) 与成交方向 (内盘/外盘)。
 *
 * 数据来自 GET /api/quote/book: 与连板梯队封单走同一条 depth5 能力路由与限速,
 * 单只按需拉取, 不进盘中轮询热路径。
 *
 * 挂载位置: 个股详情弹窗的**日K视图**里, 竖排在日K右侧那张分时图 (点某日后出现的
 * StockIntradayChart) 旁边并排, 与分时图等高 —— 盘口与成交方向都是当日实时/当日累计
 * 数据, 属于当日视角, 所以跟当日分时图走, 不跟历史日K, 也不进分时 tab 的多日图。
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

/** 一档: 标签 | 价 | 量(手); 买一那行加分隔线, 与卖档分组 */
function LevelRow({ side, label, price, volume, divider }: {
  side: 'ask' | 'bid'
  label: string
  price: number | null | undefined
  volume: number | null | undefined
  divider?: boolean
}) {
  return (
    <div
      className={cn(
        'flex items-center gap-1.5 rounded px-1 py-[3px]',
        divider && 'mt-0.5 border-t border-border/50 pt-1',
      )}
      title={`${label} ${price == null ? '—' : `${price.toFixed(2)}元`} / ${volume == null ? '—' : `${volume}手`}`}
    >
      <span className="w-6 shrink-0 text-[10px] leading-none text-muted">{label}</span>
      <span
        className={cn(
          'min-w-0 flex-1 text-right font-mono text-[11px] leading-none tabular-nums',
          price == null ? 'text-muted' : SIDE_TEXT[side],
        )}
      >
        {fmtPrice(price)}
      </span>
      <span className="w-12 shrink-0 text-right font-mono text-[10px] leading-none tabular-nums text-secondary">
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
    <div className={cn('flex flex-col overflow-hidden rounded border border-border/50 bg-elevated/25', className)}>
      {/* 头部: 标题 + 快照时间 */}
      <div className="flex items-center gap-1.5 border-b border-border/50 px-2 py-1.5">
        <span className="text-[11px] font-semibold text-foreground">盘口</span>
        <span className="text-[9px] text-muted">价 / 量(手)</span>
        <span className="ml-auto shrink-0 font-mono text-[9px] text-muted" title="盘口快照时间">
          {fmtBookTime(book?.timestamp)}
        </span>
      </div>

      {/* 五档 (或状态占位) */}
      <div className="flex-1 px-1.5 py-1">
        {q.isLoading ? (
          <div className="flex flex-col gap-1">
            {LEVELS.map(l => (
              <div key={`${l.side}${l.label}`} className="h-[17px] animate-pulse rounded bg-elevated/50" />
            ))}
          </div>
        ) : available === false ? (
          <div className="flex flex-col items-start gap-1.5 py-1">
            <MissingCapChip capKey="depth5" to={null} />
            <span className="text-[10px] leading-relaxed text-muted">
              当前数据源未提供五档盘口, 可在「设置 → 数据源」把五档盘口指向声明该能力的数据源。
            </span>
          </div>
        ) : q.isError ? (
          <p className="py-1 text-[10px] text-muted">盘口读取失败, 稍后重试。</p>
        ) : book == null ? (
          <p className="py-1 text-[10px] leading-relaxed text-muted">
            该股暂无盘口 (非交易时段 / 停牌 / 数据源本次未返回)。
          </p>
        ) : (
          <div className="flex flex-col">
            {LEVELS.map(l => {
              const prices = l.side === 'ask' ? book.ask_prices : book.bid_prices
              const volumes = l.side === 'ask' ? book.ask_volumes : book.bid_volumes
              return (
                <LevelRow
                  key={`${l.side}${l.label}`}
                  side={l.side}
                  label={l.label}
                  price={prices?.[l.idx]}
                  volume={volumes?.[l.idx]}
                  divider={l.side === 'bid' && l.idx === 0}
                />
              )
            })}
          </div>
        )}
      </div>

      {/* 成交方向 (可选协议; 缺失时整段不渲染) */}
      {hasFlow && (
        <div className="border-t border-border/50 px-2 py-1.5">
          <div className="flex items-center justify-between text-[10px] leading-none">
            <span className="text-muted">内盘</span>
            <span className="font-mono tabular-nums text-bear">{fmtBigNum(inside)}</span>
          </div>
          <div className="mt-1 flex items-center justify-between text-[10px] leading-none">
            <span className="text-muted">外盘</span>
            <span className="font-mono tabular-nums text-bull">{fmtBigNum(outside)}</span>
          </div>
          {buyShare != null && (
            <>
              <div
                className="mt-1.5 flex h-1 overflow-hidden rounded-full bg-bear/40"
                title="外盘 ÷ (内盘 + 外盘): 主动买占比, 越高越偏买方"
              >
                <div className="h-full rounded-full bg-bull" style={{ width: `${(buyShare * 100).toFixed(1)}%` }} />
              </div>
              <div className="mt-1 flex items-center justify-between text-[9px] leading-none text-muted">
                <span>主动买占比</span>
                <span className="font-mono tabular-nums text-foreground/80">{(buyShare * 100).toFixed(1)}%</span>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
