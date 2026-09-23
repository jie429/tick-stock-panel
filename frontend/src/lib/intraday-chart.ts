import type { MinuteKlineRow } from '@/lib/api'

export type DailySummary = { date: string; open: number | null; high: number; low: number; close: number }

/** 仅汇总实际收到的分钟，不将缺失开盘价或成交额伪装为真实值。 */
export function summarizeMinutes(data: MinuteKlineRow[]): MinuteKlineRow | null {
  if (!data.length) return null
  return {
    datetime: data[0].datetime,
    open: data[0].open,
    high: Math.max(...data.map(row => row.high)),
    low: Math.min(...data.map(row => row.low)),
    close: data[data.length - 1].close,
    volume: data.reduce((total, row) => total + row.volume, 0),
    amount: data.every(row => row.amount != null && Number.isFinite(row.amount))
      ? data.reduce((total, row) => total + row.amount!, 0) : null,
  }
}

/** 从 datetime 串取 HH:MM。契约: 分钟K datetime 已在后端入口统一为北京墙钟, 前端不做时区换算。 */
export function formatMinuteTime(datetime: string): string {
  const match = datetime.match(/(\d{2}):(\d{2})/)
  if (!match) return datetime.slice(11, 16)
  return `${match[1]}:${match[2]}`
}

/** 集合竞价时刻: 竞价终态 09:25 撮合一笔, 是分时图全天第一格 (通达信口径)。 */
export const AUCTION_TIME = '09:25'

/** 合成竞价柱所需的最小字段 (GET /api/quote/auction 的 item 子集)。 */
export interface AuctionBarSource {
  open_price?: number | null
  auction_volume?: number | null
  auction_amount?: number | null
}

/**
 * 由竞价快照合成 09:25 竞价柱: 竞价全部按同一价格撮合, 故 OHLC 同为今开。
 *
 * 量额取竞价快照原值 (手 / 元), 缺量或价非法时返回 null —— 宁可不画柱, 也不补 0
 * 现造一根「零成交」的 K 线。返回的 datetime 为北京墙钟 naive (与分钟K同一契约)。
 */
export function buildAuctionBar(
  date: string, item: AuctionBarSource | null | undefined,
): MinuteKlineRow | null {
  const price = item?.open_price
  const volume = item?.auction_volume
  if (typeof price !== 'number' || !Number.isFinite(price) || price <= 0) return null
  if (typeof volume !== 'number' || !Number.isFinite(volume) || volume < 0) return null
  const amount = item?.auction_amount
  return {
    datetime: `${date} ${AUCTION_TIME}:00`,
    open: price,
    high: price,
    low: price,
    close: price,
    volume,
    amount: typeof amount === 'number' && Number.isFinite(amount) ? amount : null,
  }
}

/**
 * 把竞价柱插到分时最前, 并从首根分钟柱扣减其竞价部分。
 *
 * 源分钟K把集合竞价并入了当日首根分钟柱 (实测: Σ分钟柱 ≡ 当日全天量额), 所以竞价
 * 单独成柱时必须从首根扣掉这一份, 否则量柱、累计量额与均价线都会把竞价再算一遍。
 * 扣减后全天合计与均价线口径不变, 只是把竞价成交挪回它真正发生的 09:25。
 */
export function prependAuctionBar(rows: MinuteKlineRow[], bar: MinuteKlineRow): MinuteKlineRow[] {
  if (!rows.length) return [bar]
  const [first, ...rest] = rows
  const amount = first.amount == null || bar.amount == null
    ? first.amount
    : Math.max(0, first.amount - bar.amount)
  return [
    bar,
    { ...first, volume: Math.max(0, first.volume - bar.volume), amount },
    ...rest,
  ]
}

export function computeIntradayAverage(data: MinuteKlineRow[]): (number | null)[] {
  const result: (number | null)[] = []
  let amount = 0
  let volume = 0
  let hasAmount = true
  for (const row of data) {
    if (typeof row.amount === 'number' && Number.isFinite(row.amount)) {
      amount += row.amount
    } else {
      hasAmount = false
    }
    volume += row.volume * 100
    result.push(hasAmount && volume > 0 ? amount / volume : null)
  }
  return result
}

function generateFullDayTimes(): string[] {
  const times: string[] = []
  for (let hour = 9; hour <= 11; hour++) {
    const startMinute = hour === 9 ? 30 : 0
    const endMinute = hour === 11 ? 30 : 59
    for (let minute = startMinute; minute <= endMinute; minute++) {
      times.push(`${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`)
    }
  }
  for (let hour = 13; hour <= 15; hour++) {
    const endMinute = hour === 15 ? 0 : 59
    for (let minute = 0; minute <= endMinute; minute++) {
      times.push(`${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`)
    }
  }
  return times
}

export const FULL_DAY_TIMES = generateFullDayTimes()

/**
 * 分时时间轴: 数据里含 09:25 竞价柱时前置该格, 否则用默认全天时轴。
 *
 * 分时图按固定全天时轴取位 (09:30 起), 轴槽位与柱由同一份数据决定, 避免 09:25 的柱
 * 落在轴外被 ECharts 静默丢弃。
 */
export function intradayTimes(data: MinuteKlineRow[]): string[] {
  const hasAuction = data.some(row => formatMinuteTime(row.datetime) === AUCTION_TIME)
  return hasAuction ? [AUCTION_TIME, ...FULL_DAY_TIMES] : FULL_DAY_TIMES
}
