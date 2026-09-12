import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Crown, Database, Play, RefreshCw, Trash2 } from 'lucide-react'

import { DatePicker } from '@/components/DatePicker'
import { PageHeader } from '@/components/PageHeader'
import type { FrontendExtension } from '@/extensions/types'
import {
  api,
  type DragonBacktestDetail,
  type DragonBacktestRequest,
  type DragonEquityPoint,
  type DragonScanDetail,
  type DragonScanRequest,
} from '@/lib/api'
import { QK } from '@/lib/queryKeys'

type Tab = 'scan' | 'backtest'

const QUALITY_LABELS: Record<string, string> = {
  candidate_minute: '候选股分钟线缺失',
  leading_industry_minute: '候选相关行业分钟样本不足',
  market_index_minute: '上证指数分钟线或昨收基准缺失',
  depth5_sealed_snapshot: '当日五档封单快照缺失，流动性按中性值估算',
  candidate_depth5_snapshot: '部分候选五档封单缺失，流动性按中性值估算',
  absorption_minute_history: '资金承接分钟历史缺失，按中性值估算',
  candidate_absorption_history: '部分候选行业承接历史不足，按中性值估算',
}

function qualityLabels(values: string[]) {
  return values.map(value => QUALITY_LABELS[value] ?? value).join('、')
}

function localDate(offsetDays = 0) {
  const value = new Date()
  value.setDate(value.getDate() + offsetDays)
  const year = value.getFullYear()
  const month = String(value.getMonth() + 1).padStart(2, '0')
  const day = String(value.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function NumberField({
  label,
  value,
  onChange,
  min,
  max,
  step = 1,
}: {
  label: string
  value: number
  onChange: (value: number) => void
  min?: number
  max?: number
  step?: number
}) {
  return (
    <label className="flex flex-col gap-1 text-xs text-muted">
      <span>{label}</span>
      <input
        type="number"
        value={value}
        min={min}
        max={max}
        step={step}
        onChange={event => onChange(Number(event.target.value))}
        className="h-8 w-full rounded-input border border-border bg-elevated px-2 text-xs text-foreground outline-none focus:border-accent/60"
      />
    </label>
  )
}

function EmptyState({ children }: { children: string }) {
  return (
    <div className="flex min-h-40 items-center justify-center rounded-card border border-dashed border-border text-sm text-muted">
      {children}
    </div>
  )
}

function QualityBanner({ detail }: { detail: DragonScanDetail }) {
  const quality = detail.data_quality
  const critical = quality.critical_missing ?? (quality.complete ? [] : quality.missing)
  const optional = quality.optional_missing ?? []
  const degraded = quality.complete && optional.length > 0
  const tone = quality.complete
    ? degraded
      ? 'border-warning/30 bg-warning/5 text-warning'
      : 'border-accent/30 bg-accent/5 text-accent'
    : 'border-danger/30 bg-danger/5 text-danger'
  return (
    <div className={`rounded-card border px-3 py-2 text-xs ${tone}`}>
      <div className="font-medium">
        {quality.complete
          ? degraded
            ? `关键数据已通过，以下维度采用降级评分：${qualityLabels(optional)}`
            : '数据完整性门控已通过'
          : `关键数据不完整，结果不会认证为真龙：${qualityLabels(critical) || '未知缺项'}`}
      </div>
      <div className="mt-1 text-muted">
        行业源：{quality.industry_source || '未识别'} · 股票分钟：{quality.minute_rows ?? 0} 行
        · 市场基准：{quality.market_source || '未获取'}（{quality.market_minute_rows ?? 0} 行）
      </div>
      {(quality.minute_symbols_requested ?? 0) > 0 ? (
        <div className="mt-1 text-muted">
          行业样本标的：{quality.minute_symbols_requested} 只
          {quality.minute_symbols_fetched ? ` · 本次按需补取 ${quality.minute_symbols_fetched} 只` : ''}
        </div>
      ) : null}
      {quality.fetch_errors?.length ? (
        <div className="mt-1 text-danger">分钟补取异常：{quality.fetch_errors.join('；')}</div>
      ) : null}
      {quality.depth_fetch_attempted && quality.depth_fetch_result?.msg ? (
        <div className={`mt-1 ${quality.depth_fetch_result.ok ? 'text-muted' : 'text-danger'}`}>
          五档盘口补取：{quality.depth_fetch_result.msg}
        </div>
      ) : null}
    </div>
  )
}

function ScanPanel({ disabled = false }: { disabled?: boolean }) {
  const queryClient = useQueryClient()
  const [selectedId, setSelectedId] = useState('')
  const [form, setForm] = useState<DragonScanRequest>({
    as_of: localDate(),
    top_industries: 5,
    lagging_industries: 20,
    industry_level: 2,
    result_limit: 25,
    absorption_days: 10,
  })
  const list = useQuery({ queryKey: QK.dragonQuantScans, queryFn: api.dragonQuantScans })
  useEffect(() => {
    if (!selectedId && list.data?.[0]) setSelectedId(list.data[0].id)
  }, [list.data, selectedId])
  const detail = useQuery({
    queryKey: QK.dragonQuantScan(selectedId),
    queryFn: () => api.dragonQuantScan(selectedId),
    enabled: Boolean(selectedId),
  })
  const run = useMutation({
    mutationFn: api.dragonQuantRunScan,
    onSuccess: value => {
      queryClient.setQueryData(QK.dragonQuantScan(value.id), value)
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantScans })
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantStatus })
      setSelectedId(value.id)
    },
  })
  const remove = useMutation({
    mutationFn: api.dragonQuantDeleteScan,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantScans })
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantStatus })
      setSelectedId('')
    },
  })

  return (
    <div className="grid gap-4 xl:grid-cols-[300px_minmax(0,1fr)]">
      <aside className="space-y-3">
        <div className="rounded-card border border-border bg-surface p-3">
          <div className="mb-3 text-sm font-medium">运行五维扫描</div>
          <div className="space-y-3">
            <label className="flex flex-col gap-1 text-xs text-muted">
              <span>交易日期</span>
              <DatePicker value={form.as_of} max={localDate()} onChange={as_of => setForm(current => ({ ...current, as_of }))} align="left" />
            </label>
            <div className="grid grid-cols-2 gap-2">
              <NumberField label="领涨行业" value={form.top_industries} min={1} max={20} onChange={top_industries => setForm(current => ({ ...current, top_industries }))} />
              <NumberField label="领跌行业" value={form.lagging_industries} min={2} max={50} onChange={lagging_industries => setForm(current => ({ ...current, lagging_industries }))} />
              <NumberField label="行业层级" value={form.industry_level} min={1} max={5} onChange={industry_level => setForm(current => ({ ...current, industry_level }))} />
              <NumberField label="返回数量" value={form.result_limit} min={1} max={100} onChange={result_limit => setForm(current => ({ ...current, result_limit }))} />
              <NumberField label="承接回看日" value={form.absorption_days} min={3} max={30} onChange={absorption_days => setForm(current => ({ ...current, absorption_days }))} />
            </div>
            <button
              type="button"
              disabled={disabled || run.isPending || !form.as_of}
              title={disabled ? '行业扩展数据未就绪' : undefined}
              onClick={() => run.mutate(form)}
              className="inline-flex h-8 w-full items-center justify-center gap-2 rounded-btn bg-accent px-3 text-xs font-medium text-white disabled:cursor-not-allowed disabled:opacity-50"
            >
              {run.isPending ? <RefreshCw className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
              {run.isPending ? '扫描中' : '运行扫描'}
            </button>
          </div>
        </div>
        <div className="rounded-card border border-border bg-surface p-3">
          <div className="mb-2 text-sm font-medium">历史扫描</div>
          {list.isLoading ? <div className="text-xs text-muted">加载中...</div> : !list.data?.length ? (
            <div className="text-xs text-muted">暂无扫描记录</div>
          ) : (
            <div className="max-h-72 space-y-1 overflow-auto">
              {list.data.map(item => (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setSelectedId(item.id)}
                  className={`flex w-full items-center justify-between rounded-btn px-2 py-2 text-left text-xs ${selectedId === item.id ? 'bg-accent/15 text-accent' : 'hover:bg-elevated'}`}
                >
                  <span>{item.as_of}</span>
                  <span>{item.summary.true_dragon_count} 只真龙</span>
                </button>
              ))}
            </div>
          )}
        </div>
      </aside>

      <section className="min-w-0 space-y-3">
        {detail.isLoading ? <EmptyState>正在加载扫描结果...</EmptyState> : detail.isError ? (
          <EmptyState>扫描记录加载失败</EmptyState>
        ) : !detail.data ? <EmptyState>选择或运行一次五维扫描</EmptyState> : (
          <>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <div className="text-base font-medium">{detail.data.as_of} 龙头候选</div>
                <div className="text-xs text-muted">
                  候选 {detail.data.summary.candidate_count} · 评分过线 {detail.data.summary.score_passed_count} · 真龙 {detail.data.summary.true_dragon_count}
                </div>
              </div>
              <button
                type="button"
                disabled={remove.isPending}
                onClick={() => window.confirm('确认删除这条扫描记录？') && remove.mutate(detail.data.id)}
                className="inline-flex h-8 items-center gap-1 rounded-btn border border-border px-2.5 text-xs text-muted hover:border-danger/50 hover:text-danger"
              >
                <Trash2 className="h-3.5 w-3.5" />删除
              </button>
            </div>
            <QualityBanner detail={detail.data} />
            {!detail.data.rows.length ? <EmptyState>当日领涨行业内没有符合口径的涨停候选</EmptyState> : (
              <div className="overflow-auto rounded-card border border-border bg-surface">
                <table className="w-full min-w-[980px] text-xs">
                  <thead className="bg-elevated text-muted">
                    <tr>
                      {['排名', '股票', '行业', '连板', '综合分', '带动', '领涨', '抗跌', '流动', '承接', '结论'].map(label => (
                        <th key={label} className="px-3 py-2 text-left font-medium">{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {detail.data.rows.map(row => (
                      <tr key={row.symbol} className="border-t border-border/70 hover:bg-elevated/40">
                        <td className="px-3 py-2 num">{row.rank ?? '—'}</td>
                        <td className="px-3 py-2"><div className="font-medium">{row.name}</div><div className="text-muted num">{row.symbol}</div></td>
                        <td className="px-3 py-2">{row.industry}</td>
                        <td className="px-3 py-2 num">{row.board_count}</td>
                        <td className="px-3 py-2 font-medium num">{row.composite_score.toFixed(2)}</td>
                        {(['drive', 'leadership', 'anti_drop', 'liquidity', 'absorption'] as const).map(key => (
                          <td key={key} className="px-3 py-2 num">{row.dimensions[key].score.toFixed(1)}</td>
                        ))}
                        <td className="max-w-64 px-3 py-2">
                          {row.is_true_dragon ? <span className="text-accent">真龙</span> : <span className="text-danger">{row.reject_reason || '未通过'}</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </section>
    </div>
  )
}

function EquityChart({ points }: { points: DragonEquityPoint[] }) {
  const polyline = useMemo(() => {
    if (!points.length) return ''
    const values = points.map(point => point.equity)
    const low = Math.min(...values)
    const high = Math.max(...values)
    const span = Math.max(high - low, 1)
    return points.map((point, index) => {
      const x = points.length === 1 ? 0 : index / (points.length - 1) * 100
      const y = 94 - (point.equity - low) / span * 88
      return `${x},${y}`
    }).join(' ')
  }, [points])
  if (!points.length) return <EmptyState>暂无权益曲线</EmptyState>
  return (
    <div className="h-56 rounded-card border border-border bg-surface p-3">
      <div className="mb-2 text-sm font-medium">账户权益曲线</div>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="h-[180px] w-full overflow-visible">
        <line x1="0" y1="94" x2="100" y2="94" stroke="currentColor" className="text-border" strokeWidth="0.5" />
        <polyline points={polyline} fill="none" stroke="currentColor" className="text-accent" strokeWidth="1.6" vectorEffect="non-scaling-stroke" />
      </svg>
    </div>
  )
}

function BacktestResult({ value }: { value: DragonBacktestDetail }) {
  const stats = value.stats
  return (
    <div className="space-y-3">
      {value.warnings.map(warning => (
        <div key={warning} className="rounded-card border border-warning/30 bg-warning/5 px-3 py-2 text-xs text-warning">{warning}</div>
      ))}
      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        {[
          ['总收益', `${stats.total_return_pct.toFixed(2)}%`],
          ['最大回撤', `${stats.max_drawdown_pct.toFixed(2)}%`],
          ['最终权益', stats.final_equity.toLocaleString('zh-CN', { maximumFractionDigits: 2 })],
          ['交易笔数', String(stats.trade_count)],
        ].map(([label, valueText]) => (
          <div key={label} className="rounded-card border border-border bg-surface p-3">
            <div className="text-xs text-muted">{label}</div>
            <div className="mt-1 text-lg font-semibold num">{valueText}</div>
          </div>
        ))}
      </div>
      <EquityChart points={value.equity_curve} />
      <div className="overflow-auto rounded-card border border-border bg-surface">
        <div className="border-b border-border px-3 py-2 text-sm font-medium">交割记录</div>
        {!value.trades.length ? <div className="p-6 text-center text-xs text-muted">区间内没有触发交易</div> : (
          <table className="w-full min-w-[850px] text-xs">
            <thead className="bg-elevated text-muted"><tr>{['日期', '股票', '方向', '价格', '数量', '费用', '原因'].map(label => <th key={label} className="px-3 py-2 text-left font-medium">{label}</th>)}</tr></thead>
            <tbody>{value.trades.map((trade, index) => (
              <tr key={`${trade.trade_date}-${trade.symbol}-${index}`} className="border-t border-border/70">
                <td className="px-3 py-2 num">{trade.trade_date}</td>
                <td className="px-3 py-2">{trade.name}<span className="ml-1 text-muted num">{trade.symbol}</span></td>
                <td className={`px-3 py-2 ${trade.side === 'buy' ? 'text-bull' : 'text-bear'}`}>{trade.side === 'buy' ? '买入' : '卖出'}</td>
                <td className="px-3 py-2 num">{trade.price.toFixed(3)}</td>
                <td className="px-3 py-2 num">{trade.quantity}</td>
                <td className="px-3 py-2 num">{trade.fee.toFixed(2)}</td>
                <td className="px-3 py-2">{trade.reason_text}</td>
              </tr>
            ))}</tbody>
          </table>
        )}
      </div>
    </div>
  )
}

function BacktestPanel() {
  const queryClient = useQueryClient()
  const [selectedId, setSelectedId] = useState('')
  const [form, setForm] = useState<DragonBacktestRequest>({
    start: localDate(-90),
    end: localDate(),
    initial_cash: 100_000,
    candidate_top_n: 5,
    candidate_lookback_days: 3,
    max_positions: 5,
    min_score: 50,
    min_amount: 200_000_000,
    min_turnover: 5,
    first_day_stop_loss_pct: -3.5,
    stop_loss_pct: -5,
    breakeven_activate_pct: 6,
    trailing_activate_pct: 8,
    trailing_drawdown_pct: 3.5,
    auto_scan_missing: false,
  })
  const list = useQuery({ queryKey: QK.dragonQuantBacktests, queryFn: api.dragonQuantBacktests })
  useEffect(() => {
    if (!selectedId && list.data?.[0]) setSelectedId(list.data[0].id)
  }, [list.data, selectedId])
  const detail = useQuery({
    queryKey: QK.dragonQuantBacktest(selectedId),
    queryFn: () => api.dragonQuantBacktest(selectedId),
    enabled: Boolean(selectedId),
  })
  const run = useMutation({
    mutationFn: api.dragonQuantRunBacktest,
    onSuccess: value => {
      queryClient.setQueryData(QK.dragonQuantBacktest(value.id), value)
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantBacktests })
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantScans })
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantStatus })
      setSelectedId(value.id)
    },
  })
  const remove = useMutation({
    mutationFn: api.dragonQuantDeleteBacktest,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantBacktests })
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantStatus })
      setSelectedId('')
    },
  })

  return (
    <div className="space-y-4">
      <div className="rounded-card border border-border bg-surface p-3">
        <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
          <label className="flex flex-col gap-1 text-xs text-muted"><span>开始日期</span><DatePicker value={form.start} max={form.end} onChange={start => setForm(current => ({ ...current, start }))} align="left" /></label>
          <label className="flex flex-col gap-1 text-xs text-muted"><span>结束日期</span><DatePicker value={form.end} min={form.start} max={localDate()} onChange={end => setForm(current => ({ ...current, end }))} align="left" /></label>
          <NumberField label="初始资金" value={form.initial_cash} min={10_000} step={10_000} onChange={initial_cash => setForm(current => ({ ...current, initial_cash }))} />
          <NumberField label="最大持仓" value={form.max_positions} min={1} max={20} onChange={max_positions => setForm(current => ({ ...current, max_positions }))} />
          <NumberField label="候选数量" value={form.candidate_top_n} min={1} max={20} onChange={candidate_top_n => setForm(current => ({ ...current, candidate_top_n }))} />
          <NumberField label="候选回看日" value={form.candidate_lookback_days} min={1} max={10} onChange={candidate_lookback_days => setForm(current => ({ ...current, candidate_lookback_days }))} />
          <NumberField label="最低综合分" value={form.min_score} min={0} max={100} onChange={min_score => setForm(current => ({ ...current, min_score }))} />
          <NumberField label="最低成交额" value={form.min_amount} min={0} step={10_000_000} onChange={min_amount => setForm(current => ({ ...current, min_amount }))} />
          <NumberField label="最低换手率 %" value={form.min_turnover} min={0} step={0.5} onChange={min_turnover => setForm(current => ({ ...current, min_turnover }))} />
          <NumberField label="首日止损 %" value={form.first_day_stop_loss_pct} min={-30} max={0} step={0.5} onChange={first_day_stop_loss_pct => setForm(current => ({ ...current, first_day_stop_loss_pct }))} />
          <NumberField label="后续止损 %" value={form.stop_loss_pct} min={-30} max={0} step={0.5} onChange={stop_loss_pct => setForm(current => ({ ...current, stop_loss_pct }))} />
          <NumberField label="保本激活 %" value={form.breakeven_activate_pct} min={0} max={50} step={0.5} onChange={breakeven_activate_pct => setForm(current => ({ ...current, breakeven_activate_pct }))} />
          <NumberField label="移动止盈激活 %" value={form.trailing_activate_pct} min={0} max={100} step={0.5} onChange={trailing_activate_pct => setForm(current => ({ ...current, trailing_activate_pct }))} />
          <NumberField label="移动止盈回撤 %" value={form.trailing_drawdown_pct} min={0} max={50} step={0.5} onChange={trailing_drawdown_pct => setForm(current => ({ ...current, trailing_drawdown_pct }))} />
        </div>
        <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
          <label className="inline-flex items-center gap-2 text-xs text-muted">
            <input type="checkbox" checked={form.auto_scan_missing} onChange={event => setForm(current => ({ ...current, auto_scan_missing: event.target.checked }))} />
            自动补齐缺失扫描（最多 20 个交易日）
          </label>
          <button type="button" disabled={run.isPending || !form.start || !form.end} onClick={() => run.mutate(form)} className="inline-flex h-8 items-center gap-2 rounded-btn bg-accent px-4 text-xs font-medium text-white disabled:opacity-50">
            {run.isPending ? <RefreshCw className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}{run.isPending ? '回测中' : '运行账户回测'}
          </button>
        </div>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-2">
        <select value={selectedId} onChange={event => setSelectedId(event.target.value)} className="h-8 min-w-64 rounded-input border border-border bg-elevated px-2 text-xs">
          <option value="">选择历史回测</option>
          {list.data?.map(item => <option key={item.id} value={item.id}>{item.start} 至 {item.end} · {item.stats.total_return_pct.toFixed(2)}%</option>)}
        </select>
        {detail.data && <button type="button" disabled={remove.isPending} onClick={() => window.confirm('确认删除这条回测记录？') && remove.mutate(detail.data.id)} className="inline-flex h-8 items-center gap-1 rounded-btn border border-border px-2.5 text-xs text-muted hover:border-danger/50 hover:text-danger"><Trash2 className="h-3.5 w-3.5" />删除</button>}
      </div>
      {detail.isLoading ? <EmptyState>正在加载回测结果...</EmptyState> : detail.isError ? <EmptyState>回测记录加载失败</EmptyState> : detail.data ? <BacktestResult value={detail.data} /> : <EmptyState>选择或运行一次账户回测</EmptyState>}
    </div>
  )
}

function DragonQuantPage() {
  const [tab, setTab] = useState<Tab>('scan')
  const status = useQuery({ queryKey: QK.dragonQuantStatus, queryFn: api.dragonQuantStatus })
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="龙头策略"
        subtitle="dragon-quant 五维识别与账户级回测"
        titleExtra={status.data && <span className={`rounded-full px-2 py-0.5 text-[10px] ${status.data.status === 'ready' ? 'bg-accent/10 text-accent' : 'bg-warning/10 text-warning'}`}>{status.data.status === 'ready' ? '数据源已就绪' : '缺行业数据'}</span>}
        right={<div className="hidden items-center gap-1 text-xs text-muted md:flex"><Database className="h-3.5 w-3.5" />仅使用 tick-stock-panel 数据源</div>}
      />
      <div className="border-b border-border px-5">
        <div className="flex gap-1">
          {([['scan', '五维选股'], ['backtest', '龙头账户回测']] as const).map(([id, label]) => (
            <button key={id} type="button" onClick={() => setTab(id)} className={`border-b-2 px-3 py-2 text-sm ${tab === id ? 'border-accent text-accent' : 'border-transparent text-muted hover:text-foreground'}`}>{label}</button>
          ))}
        </div>
      </div>
      <main className="min-h-0 flex-1 overflow-auto p-5">
        {status.isError ? <div className="mb-3 rounded-card border border-danger/30 bg-danger/5 px-3 py-2 text-xs text-danger">扩展状态读取失败，请检查后端扩展是否已加载。</div> : status.data?.status === 'needs_industry_data' ? <div className="mb-3 rounded-card border border-warning/30 bg-warning/5 px-3 py-2 text-xs text-warning">未发现行业扩展数据，请先在“数据管理 / 行业分析”完成行业映射同步。</div> : null}
        {tab === 'scan' ? (
          <ScanPanel disabled={status.isLoading || status.data?.status !== 'ready'} />
        ) : <BacktestPanel />}
        <div className="mt-4 rounded-card border border-border bg-surface px-3 py-2 text-[11px] leading-5 text-muted">
          策略算法移植自 gitBingxu/dragon-quant 0.5.1（MIT）。未接入雪球、同花顺或腾讯直连；候选与有限行业样本分钟线按需读取，市场基准使用上证指数。关键分钟数据缺失时 fail-closed，五档盘口和资金承接历史缺失时按源策略中性降级；账户回测严格使用历史扫描、T+1、费用和滑点，不使用事后最低价买入。
        </div>
      </main>
    </div>
  )
}

const extension: FrontendExtension = {
  id: 'dragon.quant',
  apiVersion: 1,
  routes: [{ id: 'dragon-quant', path: '/dragon-quant', component: DragonQuantPage }],
  navigation: [{ id: 'dragon-quant', routeId: 'dragon-quant', label: '龙头策略', icon: Crown, order: 350 }],
}

export default extension

