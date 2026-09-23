import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { api, type MinuteKlineRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { klineMinuteQueryOptions, minuteRefetchInterval } from '@/lib/kline'
import { EChartsIntraday } from '@/components/EChartsIntraday'
import { buildAuctionBar, prependAuctionBar, type DailySummary } from '@/lib/intraday-chart'

interface Props {
  symbol: string
  date: string | null
  height?: number
  prevClose?: number
  dailySummary?: DailySummary
  className?: string
  onPriceHover?: (price: number | null) => void
  onPriceDoubleClick?: (price: number, currentPrice: number) => void
  currentPrice?: number
  priceLines?: { value: number; label?: string; color?: string }[]
  /** 自动刷新间隔(ms)。undefined/0 = 不轮询(默认)。个股对话框盘中实时刷新时传入。 */
  refetchIntervalMs?: number
}

export function StockIntradayChart({
  symbol,
  date,
  height = 520,
  prevClose,
  dailySummary,
  className,
  onPriceHover,
  onPriceDoubleClick,
  currentPrice,
  priceLines,
  refetchIntervalMs,
}: Props) {
  const qc = useQueryClient()
  const [minuteDismissed, setMinuteDismissed] = useState(false)
  /** 集合竞价柱开关 (默认显示; 竞价读数是否可得由后端归档决定) */
  const [showAuction, setShowAuction] = useState(true)

  const minute = useQuery({
    // 轮询上下文 (个股详情) 传 live: 当日盘中后端直接实时拉取最新K,
    // 避免读到分钟增量落盘的上一轮本地分区; 历史日期后端自行忽略 live。
    ...klineMinuteQueryOptions(symbol, date ?? undefined, refetchIntervalMs != null),
    enabled: !!symbol && !!date,
    refetchInterval: minuteRefetchInterval(refetchIntervalMs),
  })

  const fetchMinute = useMutation({
    mutationFn: () => api.syncMinuteSingle(symbol),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['kline-minute', symbol] })
      qc.invalidateQueries({ queryKey: QK.klineMinute(symbol, date ?? '') })
      setMinuteDismissed(false)
    },
  })

  // 集合竞价读数 (09:25 终态快照 + 竞价量比): 与分钟K同一个交易日, 竞价字段当日起不再
  // 变化 → 不轮询; 只有竞价尚未结束 (not_ready) 时才按 60s 复查一次, 免得开盘前打开的
  // 弹窗一直停在空态。
  const auction = useQuery({
    queryKey: QK.quoteAuction(symbol, date ?? ''),
    queryFn: () => api.quoteAuction(symbol, date ?? undefined),
    enabled: !!symbol && !!date,
    staleTime: 5 * 60_000,
    refetchInterval: query => (query.state.data?.state === 'not_ready' ? 60_000 : false),
  })
  const auctionItem = auction.data?.item ?? null

  const minuteRows: MinuteKlineRow[] = useMemo(() => minute.data?.rows ?? [], [minute.data?.rows])
  // 竞价柱插在分时最前, 并从首根分钟柱扣掉竞价那一份 (源分钟K把 09:25 竞价并入了首根柱)
  const displayRows: MinuteKlineRow[] = useMemo(() => {
    const bar = showAuction ? buildAuctionBar(date ?? '', auctionItem) : null
    return bar ? prependAuctionBar(minuteRows, bar) : minuteRows
  }, [minuteRows, showAuction, date, auctionItem])
  // source=none 表示本地无数据且 TickFlow 也拉不到 (停牌/复牌延迟/非交易日)
  // 此时不弹"是否获取"询问窗, 只做静态提示, 避免误导用户去拉明知拉不到的数据
  const sourceIsNone = minute.data?.source === 'none'
  // 指数分钟K无本地存储且不支持落库获取 (后端 sync_minute_single 显式拒绝), 不显示获取按钮
  const isIndex = minute.data?.asset_type === 'index'

  useEffect(() => {
    setMinuteDismissed(false)
    onPriceHover?.(null)
  }, [date, onPriceHover])

  if (!symbol || !date) return null

  return (
    <div className={className} style={{ height, flexShrink: 0 }}>
      {minute.isLoading && <div className="text-xs text-muted py-2">分时加载中…</div>}
      {!minute.isLoading && minuteRows.length === 0 && (
        <>
          {fetchMinute.isPending ? (
            <div className="flex items-center justify-center h-full gap-2 text-xs text-accent">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              <span>正在获取分钟K数据…</span>
            </div>
          ) : isIndex ? (
            // 指数: 分钟K仅支持实时读取, 无落库获取入口
            <div className="flex items-center justify-center h-full text-xs text-muted">指数暂无分钟数据</div>
          ) : sourceIsNone ? (
            // 数据源确认无此日分钟数据 (停牌/复牌延迟等): 静态提示 + 保留重试
            <div className="flex flex-col items-center justify-center h-full gap-3">
              <div className="text-xs text-muted">该日暂无分钟数据（数据源未提供）</div>
              <button
                onClick={() => fetchMinute.mutate()}
                className="px-4 py-1.5 rounded-btn bg-elevated text-secondary text-xs font-medium hover:bg-elevated/80 transition-colors duration-150"
              >
                重新获取
              </button>
            </div>
          ) : minuteDismissed ? (
            <div className="flex flex-col items-center justify-center h-full gap-3">
              <div className="text-xs text-muted">暂无分钟数据</div>
              <button
                onClick={() => setMinuteDismissed(false)}
                className="px-4 py-1.5 rounded-btn bg-accent/90 text-base text-xs font-medium hover:bg-accent transition-colors duration-150"
              >
                获取分钟K
              </button>
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-full gap-4">
              <div className="text-sm text-foreground">是否立即获取最近5日分钟K？</div>
              <div className="flex items-center gap-3">
                <button
                  onClick={() => fetchMinute.mutate()}
                  className="px-4 py-1.5 rounded-btn bg-accent/90 text-base text-xs font-medium hover:bg-accent transition-colors duration-150"
                >
                  确定
                </button>
                <button
                  onClick={() => setMinuteDismissed(true)}
                  className="px-4 py-1.5 rounded-btn bg-elevated text-secondary text-xs hover:bg-elevated/80 transition-colors duration-150"
                >
                  取消
                </button>
              </div>
            </div>
          )}
        </>
      )}
      {minuteRows.length > 0 && (
        <EChartsIntraday
          data={displayRows}
          height={height}
          prevClose={minute.data?.prev_close ?? prevClose}
          dailySummary={dailySummary}
          date={date}
          priceLimit={minute.data?.price_limit ?? undefined}
          onPriceHover={onPriceHover}
          onPriceDoubleClick={onPriceDoubleClick}
          currentPrice={currentPrice}
          priceLines={priceLines}
          auctionToggle={{
            item: auctionItem,
            baselineDate: auction.data?.baseline_date ?? null,
            message: auction.data?.message ?? null,
            pending: auction.isLoading,
            active: showAuction,
            onToggle: () => setShowAuction(v => !v),
          }}
        />
      )}
    </div>
  )
}
