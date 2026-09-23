// @vitest-environment node
import { describe, expect, it } from 'vitest'
import {
  AUCTION_TIME,
  buildAuctionBar,
  formatMinuteTime,
  FULL_DAY_TIMES,
  intradayTimes,
  prependAuctionBar,
} from './intraday-chart'

const bar = (over: Partial<ReturnType<typeof baseBar>> = {}) => ({ ...baseBar(), ...over })

function baseBar() {
  return {
    datetime: '2026-09-23 09:25:00',
    open: 10, high: 10, low: 10, close: 10,
    volume: 60, amount: 600000,
  }
}

function minute(time: string, volume: number, amount: number | null = volume * 1000) {
  return {
    datetime: `2026-09-23 ${time}:00`,
    open: 10, high: 10, low: 10, close: 10,
    volume, amount,
  }
}

describe('buildAuctionBar: 竞价快照 → 一根 09:25 柱', () => {
  it('竞价按同一价格撮合, OHLC 同为今开; 量额取快照原值(手/元)', () => {
    expect(buildAuctionBar('2026-09-23', {
      open_price: 12.34, auction_volume: 63.1, auction_amount: 7919200,
    })).toEqual({
      datetime: '2026-09-23 09:25:00',
      open: 12.34, high: 12.34, low: 12.34, close: 12.34,
      volume: 63.1, amount: 7919200,
    })
  })

  it('缺量或缺价时不造柱 —— 不补 0 现造一根零成交 K 线', () => {
    expect(buildAuctionBar('2026-09-23', { open_price: 12.34 })).toBeNull()
    expect(buildAuctionBar('2026-09-23', { open_price: 0, auction_volume: 10 })).toBeNull()
    expect(buildAuctionBar('2026-09-23', { open_price: null, auction_volume: null })).toBeNull()
    expect(buildAuctionBar('2026-09-23', null)).toBeNull()
  })
})

describe('intradayTimes: 时轴槽位与柱同源', () => {
  it('无竞价柱 → 默认全天时轴 (09:30 起)', () => {
    expect(intradayTimes([minute('09:31', 100)])).toBe(FULL_DAY_TIMES)
  })

  it('有竞价柱 → 轴前置 09:25 一格', () => {
    const times = intradayTimes([bar(), minute('09:31', 100)])
    expect(times[0]).toBe(AUCTION_TIME)
    expect(times.length).toBe(FULL_DAY_TIMES.length + 1)
    expect(times[1]).toBe(FULL_DAY_TIMES[0])
  })
})

describe('prependAuctionBar: 竞价单独成柱时从首根分钟柱扣回', () => {
  it('插到最前并扣减首根的竞价部分, 全天合计保持不变', () => {
    const rows = [minute('09:31', 634, 79598600), minute('09:32', 224, 28136600)]
    const out = prependAuctionBar(rows, bar({ volume: 63.1, amount: 7919200 }))

    expect(out.map(row => formatMinuteTime(row.datetime))).toEqual(['09:25', '09:31', '09:32'])
    expect(out[1].volume).toBeCloseTo(634 - 63.1, 6)
    expect(out[1].amount).toBeCloseTo(79598600 - 7919200, 6)
    expect(out[2]).toEqual(rows[1])
    const total = (list: { volume: number }[]) => list.reduce((sum, row) => sum + row.volume, 0)
    expect(total(out)).toBeCloseTo(total(rows), 6)
  })

  it('扣减不为负; 首根缺额时保持缺失, 不凑 0', () => {
    const out = prependAuctionBar([minute('09:31', 10, null)], bar({ volume: 60, amount: 60000 }))
    expect(out[1].volume).toBe(0)
    expect(out[1].amount).toBeNull()
  })

  it('没有分钟数据时只留竞价柱 (不抛出)', () => {
    expect(prependAuctionBar([], bar())).toHaveLength(1)
  })
})

