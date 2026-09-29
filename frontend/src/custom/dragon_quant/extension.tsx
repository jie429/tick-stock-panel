import { Fragment, useCallback, useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, Copy, Crown, Database, Play, RefreshCw, Trash2 } from 'lucide-react'

import { DatePicker } from '@/components/DatePicker'
import { PageHeader } from '@/components/PageHeader'
import { StockPreviewDialog } from '@/components/StockPreviewDialog'
import type { FrontendExtension } from '@/extensions/types'
import { api, type DragonScanDetail, type DragonScanRequest, type DragonScanRow } from '@/lib/api'
import { toNavItems } from '@/lib/listNav'
import { QK } from '@/lib/queryKeys'

const QUALITY_LABELS: Record<string, string> = {
  candidate_minute: '候选股分钟线缺失',
  leading_industry_minute: '候选相关行业分钟样本不足',
  market_index_minute: '上证指数分钟线或昨收基准缺失',
  depth5_sealed_snapshot: '当日五档封单快照缺失，流动性按中性值估算',
  candidate_depth5_snapshot: '部分候选五档封单缺失，流动性按中性值估算',
  absorption_minute_history: '资金承接分钟历史缺失，按中性值估算',
  candidate_absorption_history: '部分候选行业承接历史不足，按中性值估算',
}

const SCAN_DEFAULTS: DragonScanRequest = {
  as_of: '',
  top_industries: 5,
  lagging_industries: 20,
  industry_level: 2,
  result_limit: 25,
  absorption_days: 10,
}

const WEIGHT_TEXT = '带动 30% · 领涨 25% · 抗跌 15% · 流动 20% · 承接 10%'

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

function fmtLots(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  return value.toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function fmtAmount(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  if (Math.abs(value) >= 1e8) return `${(value / 1e8).toFixed(2)} 亿`
  if (Math.abs(value) >= 1e4) return `${(value / 1e4).toFixed(2)} 万`
  return value.toFixed(0)
}

function fmtScore(value?: number | null, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  const text = value.toFixed(digits)
  return text.includes('.') ? text.replace(/\.?0+$/, '') : text
}

function shortDay(day: string) {
  const parts = day.split('-')
  if (parts.length !== 3) return day
  return `${Number(parts[1])}月${Number(parts[2])}日`
}

/** 数字输入: 允许清空重输, 越界值在失焦时收敛到边界, 空值回退到上一个有效值。 */
function NumberField({
  label,
  value,
  onChange,
  min,
  max,
  step = 1,
  hint,
}: {
  label: string
  value: number
  onChange: (value: number) => void
  min?: number
  max?: number
  step?: number
  hint?: string
}) {
  const [text, setText] = useState(() => String(value))
  const [focused, setFocused] = useState(false)

  useEffect(() => {
    if (!focused) setText(String(value))
  }, [value, focused])

  const clamp = (input: number) => {
    let next = input
    if (min !== undefined) next = Math.max(min, next)
    if (max !== undefined) next = Math.min(max, next)
    return next
  }

  const handleText = (raw: string) => {
    setText(raw)
    if (raw.trim() === '') return
    const parsed = Number(raw)
    if (!Number.isFinite(parsed)) return
    // 输入中途越界不立即写入上层, 避免半截数字被夹到边界后光标/值来回跳
    if (min !== undefined && parsed < min) return
    if (max !== undefined && parsed > max) return
    onChange(parsed)
  }

  const commit = () => {
    setFocused(false)
    const parsed = Number(text)
    if (text.trim() === '' || !Number.isFinite(parsed)) {
      setText(String(value))
      return
    }
    const next = clamp(parsed)
    setText(String(next))
    if (next !== value) onChange(next)
  }

  return (
    <label className="flex flex-col gap-1 text-xs text-muted">
      <span>{label}</span>
      <input
        type="number"
        inputMode="numeric"
        value={text}
        min={min}
        max={max}
        step={step}
        onFocus={event => {
          setFocused(true)
          event.currentTarget.select()
        }}
        onChange={event => handleText(event.target.value)}
        onBlur={commit}
        onKeyDown={event => {
          if (event.key === 'Enter') event.currentTarget.blur()
        }}
        className="h-8 w-full rounded-input border border-border bg-elevated px-2 text-xs text-foreground outline-none focus:border-accent/60"
      />
      {hint ? <span className="text-[10px] leading-tight text-muted/80">{hint}</span> : null}
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

const DIM_CN: Record<string, string> = {
  drive: '带动性',
  leadership: '领涨性',
  anti_drop: '抗跌性',
  liquidity: '流动性',
  absorption: '资金承接',
}

function translateReject(reason: string) {
  return reason
    .replace(/\b(drive|leadership|anti_drop|liquidity|absorption)=/g, (_match, key: string) => `${DIM_CN[key] ?? key} `)
    .replace('关键数据不完整:', '关键数据不完整：')
}

function antiDropLegText(leg: { score: number; dip_segments?: number; hold_score?: number; rebound_score?: number; no_dip?: boolean; degraded?: boolean }, label: string) {
  if (leg.degraded) return `${label}分钟线缺失，按中性 65 分`
  if (leg.no_dip) return `${label}无有效跳水段，按中性 65 分`
  return `${label}跳水 ${leg.dip_segments ?? 0} 段：抗跌 ${fmtScore(leg.hold_score)} / 反弹 ${fmtScore(leg.rebound_score)}`
}

/** 把一行评分还原成「结论 + 逐维度证据」的说明文本 (与页面明细同一来源)。 */
function buildNarrative(row: DragonScanRow) {
  const drive = row.dimensions.drive
  const leadership = row.dimensions.leadership
  const antiDrop = row.dimensions.anti_drop
  const liquidity = row.dimensions.liquidity
  const absorption = row.dimensions.absorption
  const early = drive.details.early
  const lead = drive.details.lead
  const voice = drive.details.voice

  const earlyText = early.sealed
    ? `${early.seal_time ?? '—'}封板，封单量${fmtLots(early.sealed_volume_lots)}手，涨停池第${early.rank ?? '—'}/${early.pool_size ?? '—'}`
    : `当日未触及涨停价，涨停池已封${early.pool_size ?? 0}只`
  const leadText = lead.degraded
    ? `${lead.reason ?? '个股或行业分钟线缺失'}，按降级中值 40 分`
    : `带动${lead.n_lead ?? 0}次，被带${lead.n_follow ?? 0}次`
  const voiceText = voice.degraded
    ? '行业成分股快照缺失，无法评估'
    : `涨停${voice.limit_up_count ?? 0}只/强势${voice.strong_count ?? 0}只（成分${voice.member_count ?? 0}只）`

  const sealText = liquidity.details.seal_strength === null || liquidity.details.seal_strength === undefined
    ? '无五档封单，按中性 60 分'
    : `强度${fmtScore(liquidity.details.seal_strength, 2)}`
  const openText = liquidity.details.open_count === null || liquidity.details.open_count === undefined
    ? '开板次数未知'
    : `开板${liquidity.details.open_count}次`

  const absorptionEvents = absorption.details.top_events ?? []
  const absorptionText = absorption.details.fallback
    ? '回看期内没有符合条件的承接窗口（目标行业拉升 + 同期≥2个板块回落），按中性 50 分'
    : `${absorption.details.event_count ?? 0}个窗口合并为${absorption.details.merged_event_count ?? absorption.details.event_count ?? 0}次独立承接，最优窗口${fmtScore(absorption.details.best_event_score)}分（维度分 = 最优 + 窗口数奖励，上限 100）`

  const lines = [
    `🐉 带动性(${fmtScore(drive.score)}): 封板最早${fmtScore(early.score)}：${earlyText}；带动板块${fmtScore(lead.score)}：${leadText}；板块共鸣${fmtScore(voice.score)}：${voiceText}`,
    `📊 领涨性(${fmtScore(leadership.score)}): 连板${fmtScore(leadership.details.board_score)}(本${leadership.details.board_count}板/行业最高${leadership.details.industry_max_boards}板)/涨幅${fmtScore(leadership.details.five_day_rank_score)}(5日${leadership.details.five_day_return_pct}%,排名${leadership.details.five_day_rank ?? '—'}/${leadership.details.five_day_peer_count ?? '—'})`,
    `🛡️ 抗跌性(${fmtScore(antiDrop.score)}): 大盘维度${fmtScore(antiDrop.details.market.score)}：${antiDropLegText(antiDrop.details.market, '大盘')}；板块维度${fmtScore(antiDrop.details.industry.score)}：${antiDropLegText(antiDrop.details.industry, row.industry)}`,
    `💧 流动性(${fmtScore(liquidity.score)}): 换手${fmtScore(liquidity.details.turnover_score)}(${liquidity.details.turnover_rate_pct}%)/封板${fmtScore(liquidity.details.seal_score)}(${sealText},${openText})`,
    `🪙 资金承接(${fmtScore(absorption.score)}): ${absorptionText}`,
    `📎 基础数据：成交额${fmtAmount(row.amount_yuan)} · 换手率${row.turnover_rate_pct}% · 5日涨幅${row.five_day_return_pct}% · 行业层级命中：${row.industry}`,
    `⚖️ 加权：${WEIGHT_TEXT} → 综合 ${row.composite_score} 分`,
  ]

  for (const event of absorptionEvents) {
    const falling = event.falling.slice(0, 3).map(item => `${item.industry}${item.change_pct}%`).join('、')
    lines.push(`　　└ 承接事件：${shortDay(event.day)} ${event.window_start}–${event.window_end} 目标行业+${event.target_return_pct}%，同期${event.falling_count}个板块回落${falling ? `（${falling}）` : ''}`)
  }

  lines.push(row.is_true_dragon
    ? `✅ 认证为真龙，真龙排名第${row.rank ?? '—'}（带动/领涨/抗跌/流动四项硬门槛全部达标）`
    : `❌ 未认证：${row.reject_reason ? translateReject(row.reject_reason) : '未通过硬门槛'}`)

  const title = `${row.name}(${row.symbol})——${row.industry}——${row.board_count}连板-${row.composite_score}分-${row.is_true_dragon ? '✓真龙' : '✗未通过'}`
  return { title, lines, text: [title, ...lines.map(line => `- ${line}`)].join('\n') }
}

function RowDetail({ row }: { row: DragonScanRow }) {
  const narrative = useMemo(() => buildNarrative(row), [row])
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(false), 1600)
    return () => window.clearTimeout(timer)
  }, [copied])

  const copy = () => {
    const text = narrative.text
    navigator.clipboard?.writeText(text).then(() => setCopied(true)).catch(() => setCopied(false))
  }

  return (
    <div className="rounded-card border border-border bg-surface p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="text-xs font-medium text-foreground">{narrative.title}</div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted">分数括号＝该维度加权分，子项括号＝子项得分</span>
          <button
            type="button"
            onClick={copy}
            className="inline-flex h-6 items-center gap-1 rounded-btn border border-border px-2 text-[10px] text-muted hover:border-accent/50 hover:text-accent"
          >
            <Copy className="h-3 w-3" />{copied ? '已复制' : '复制说明'}
          </button>
        </div>
      </div>
      <ul className="mt-2 space-y-1 text-[11px] leading-5 text-secondary">
        {narrative.lines.map((line, index) => (
          <li key={index} className="whitespace-pre-wrap break-words">{line}</li>
        ))}
      </ul>
    </div>
  )
}

function ScanPanel({ disabled = false }: { disabled?: boolean }) {
  const queryClient = useQueryClient()
  const [selectedId, setSelectedId] = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)
  const [preview, setPreview] = useState<{ symbol: string; name: string } | null>(null)
  const [form, setForm] = useState<DragonScanRequest>(() => ({ ...SCAN_DEFAULTS, as_of: localDate() }))
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
      setExpanded(null)
    },
  })
  const remove = useMutation({
    mutationFn: api.dragonQuantDeleteScan,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantScans })
      queryClient.invalidateQueries({ queryKey: QK.dragonQuantStatus })
      setSelectedId('')
      setExpanded(null)
    },
  })

  const rows = useMemo(() => detail.data?.rows ?? [], [detail.data])
  const navList = useMemo(() => toNavItems(rows), [rows])
  const toggle = useCallback((symbol: string) => {
    setExpanded(current => (current === symbol ? null : symbol))
  }, [])

  return (
    <div className="grid gap-4 xl:grid-cols-[300px_minmax(0,1fr)]">
      <aside className="space-y-3">
        <div className="rounded-card border border-border bg-surface p-3">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-medium">运行五维扫描</div>
            <button
              type="button"
              onClick={() => setForm({ ...SCAN_DEFAULTS, as_of: localDate() })}
              className="text-[10px] text-muted hover:text-accent"
            >恢复默认</button>
          </div>
          <div className="space-y-3">
            <label className="flex flex-col gap-1 text-xs text-muted">
              <span>交易日期</span>
              <DatePicker value={form.as_of} max={localDate()} onChange={as_of => setForm(current => ({ ...current, as_of }))} align="left" />
            </label>
            <div className="grid grid-cols-2 gap-2">
              <NumberField label="领涨行业" value={form.top_industries} min={1} max={20} onChange={top_industries => setForm(current => ({ ...current, top_industries }))} hint="按行业涨幅取前 N" />
              <NumberField label="领跌行业" value={form.lagging_industries} min={2} max={50} onChange={lagging_industries => setForm(current => ({ ...current, lagging_industries }))} hint="承接对照样本" />
              <NumberField label="行业层级" value={form.industry_level} min={1} max={5} onChange={industry_level => setForm(current => ({ ...current, industry_level }))} hint="1 一级 / 2 二级" />
              <NumberField label="返回数量" value={form.result_limit} min={1} max={100} onChange={result_limit => setForm(current => ({ ...current, result_limit }))} hint="候选行上限" />
              <NumberField label="承接回看日" value={form.absorption_days} min={3} max={30} onChange={absorption_days => setForm(current => ({ ...current, absorption_days }))} hint="承接历史交易日" />
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
            {run.isError ? <div className="text-[11px] text-danger">扫描失败：{(run.error as Error).message}</div> : null}
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
                  onClick={() => { setSelectedId(item.id); setExpanded(null) }}
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
                  <span className="ml-2 text-muted/80">点击行展开证据明细，点击股票名打开日K</span>
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
            {!rows.length ? <EmptyState>当日领涨行业内没有符合口径的涨停候选</EmptyState> : (
              <div className="overflow-auto rounded-card border border-border bg-surface">
                <table className="w-full min-w-[1040px] text-xs">
                  <thead className="bg-elevated text-muted">
                    <tr>
                      {['排名', '股票', '行业', '连板', '综合分', '带动', '领涨', '抗跌', '流动', '承接', '结论', ''].map((label, index) => (
                        <th key={`${label}-${index}`} className="px-3 py-2 text-left font-medium">{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map(row => {
                      const isOpen = expanded === row.symbol
                      return (
                        <Fragment key={row.symbol}>
                          <tr
                            onClick={() => toggle(row.symbol)}
                            className={`cursor-pointer border-t border-border/70 ${isOpen ? 'bg-elevated/60' : 'hover:bg-elevated/40'}`}
                          >
                            <td className="px-3 py-2 num">{row.rank ?? '—'}</td>
                            <td className="px-3 py-2">
                              <button
                                type="button"
                                onClick={event => { event.stopPropagation(); setPreview({ symbol: row.symbol, name: row.name }) }}
                                className="text-left font-medium text-foreground underline-offset-2 hover:text-accent hover:underline"
                                title="查看日K"
                              >
                                {row.name}
                              </button>
                              <div className="text-muted num">{row.symbol}</div>
                            </td>
                            <td className="px-3 py-2">{row.industry}</td>
                            <td className="px-3 py-2 num">{row.board_count}</td>
                            <td className="px-3 py-2 font-medium num">{row.composite_score.toFixed(2)}</td>
                            {(['drive', 'leadership', 'anti_drop', 'liquidity', 'absorption'] as const).map(key => (
                              <td key={key} className="px-3 py-2 num">{row.dimensions[key].score.toFixed(1)}</td>
                            ))}
                            <td className="max-w-64 px-3 py-2">
                              {row.is_true_dragon ? <span className="text-accent">真龙</span> : <span className="text-danger">{translateReject(row.reject_reason || '未通过')}</span>}
                            </td>
                            <td className="px-2 py-2 text-muted">
                              {isOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                            </td>
                          </tr>
                          {isOpen ? (
                            <tr className="border-t border-border/70 bg-elevated/30">
                              <td colSpan={12} className="px-3 pb-3 pt-2">
                                <RowDetail row={row} />
                              </td>
                            </tr>
                          ) : null}
                        </Fragment>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </section>

      {preview ? (
        <StockPreviewDialog
          symbol={preview.symbol}
          name={preview.name}
          navList={navList}
          onClose={() => setPreview(null)}
          onNavigate={(symbol, name) => setPreview({ symbol, name: name ?? '' })}
        />
      ) : null}
    </div>
  )
}

function DragonQuantPage() {
  const status = useQuery({ queryKey: QK.dragonQuantStatus, queryFn: api.dragonQuantStatus })
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader
        title="龙头策略"
        subtitle="dragon-quant 五维真龙识别与逐票证据明细"
        titleExtra={status.data && <span className={`rounded-full px-2 py-0.5 text-[10px] ${status.data.status === 'ready' ? 'bg-accent/10 text-accent' : 'bg-warning/10 text-warning'}`}>{status.data.status === 'ready' ? '数据源已就绪' : '缺行业数据'}</span>}
        right={<div className="hidden items-center gap-1 text-xs text-muted md:flex"><Database className="h-3.5 w-3.5" />仅使用 tick-stock-panel 数据源</div>}
      />
      <main className="min-h-0 flex-1 overflow-auto p-5">
        {status.isError ? <div className="mb-3 rounded-card border border-danger/30 bg-danger/5 px-3 py-2 text-xs text-danger">扩展状态读取失败，请检查后端扩展是否已加载。</div> : status.data?.status === 'needs_industry_data' ? <div className="mb-3 rounded-card border border-warning/30 bg-warning/5 px-3 py-2 text-xs text-warning">未发现行业扩展数据，请先在“数据管理 / 行业分析”完成行业映射同步。</div> : null}
        <ScanPanel disabled={status.isLoading || status.data?.status !== 'ready'} />
        <div className="mt-4 rounded-card border border-border bg-surface px-3 py-2 text-[11px] leading-5 text-muted">
          策略算法移植自 gitBingxu/dragon-quant 0.5.1（MIT）。未接入雪球、同花顺或腾讯直连；候选与有限行业样本分钟线按需读取，市场基准使用上证指数。关键分钟数据缺失时 fail-closed，五档盘口和资金承接历史缺失时按源策略中性降级。资金承接为本地简化实现：维度分取「最优窗口分 + 窗口数奖励」，明细里标注实际使用的窗口数与代表事件。
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
