// @vitest-environment jsdom
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, expect, it, vi } from 'vitest'
import { EChartsIntraday } from './EChartsIntraday'
import { FULL_DAY_TIMES } from '@/lib/intraday-chart'

const chart = vi.hoisted(() => ({
  handlers: {} as Record<string, (event?: any) => void>,
  on: vi.fn(), off: vi.fn(), setOption: vi.fn(), clear: vi.fn(), resize: vi.fn(), dispose: vi.fn(),
  getZr: () => ({ on: vi.fn(), off: vi.fn() }),
}))
vi.mock('echarts', () => ({ init: () => chart }))
vi.mock('@/lib/theme', () => ({ useChartTheme: () => ({}) }))
const rows = [
  { datetime: '2026-09-09 09:30:00', open: 119.77, high: 119.77, low: 119, close: 119.1, volume: 100, amount: 1191000 },
  { datetime: '2026-09-09 11:20:00', open: 118.25, high: 118.28, low: 118.24, close: 118.25, volume: 55, amount: 650375 },
]
const daily = { date: '2026-09-09', open: 119.77, high: 119.77, low: 118.16, close: 118.25 }
let cleanup = async () => {}
afterEach(async () => { await cleanup(); vi.unstubAllGlobals() })

it('shows daily OHLC by default, labels hovered minute, restores on exit and date switch', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  chart.on.mockImplementation((name, callback) => { chart.handlers[name] = callback })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }
  const render = async (date = daily.date) => {
    await act(async () => root.render(<EChartsIntraday data={rows.map(row => ({ ...row, datetime: row.datetime.replace(daily.date, date) }))}
      date={date} dailySummary={{ ...daily, date }} />))
  }
  await render()
  expect(host.textContent).toContain('日K')
  expect(host.textContent).toContain('118.16')
  await act(async () => chart.handlers.updateAxisPointer({ axesInfo: [{ axisDim: 'x', value: 110 }] }))
  expect(host.textContent).toContain('11:20')
  expect(host.textContent).toContain('118.28')
  await act(async () => chart.handlers.globalout())
  expect(host.textContent).toContain('118.16')
  expect(host.textContent).not.toContain('11:20')
  await act(async () => chart.handlers.updateAxisPointer({ axesInfo: [{ axisDim: 'x', value: 110 }] }))
  await render('2026-09-10')
  expect(host.textContent).toContain('118.16')
  expect(host.textContent).not.toContain('11:20')
})

it('aggregates available minutes without inventing a missing opening price', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }
  await act(async () => root.render(<EChartsIntraday data={rows.map(row => ({ ...row, open: null }))} date={daily.date} />))
  expect(host.textContent).toContain('分时汇总')
  expect(host.textContent).toContain('119.77')
  expect(host.textContent).toContain('—')
  expect(host.textContent).toContain('155')
})

// ================================================================
// y 轴范围: 无涨跌幅新股不被钳制到 ±10% 涨跌停带内 (C沈鼓场景)
// ================================================================
const wideRows = (day: string) => [
  { datetime: `${day} 09:30:00`, open: 15, high: 15, low: 15, close: 15, volume: 100, amount: 150000 },
  { datetime: `${day} 11:20:00`, open: 57.7, high: 57.8, low: 57.7, close: 57.77, volume: 55, amount: 318000 },
]

function lastYAxis(): any {
  const call = chart.setOption.mock.calls.at(-1)
  return call?.[0]?.yAxis?.[0]
}

it('no_limit day: adaptive y-axis covers data far beyond the ±10% band', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }
  await act(async () => root.render(
    <EChartsIntraday
      data={wideRows('2026-09-18')}
      date="2026-09-18"
      prevClose={20.8}
      priceLimit={{ rate: 0.1, limit_up: null, limit_down: null, no_limit: true, source: 'rule' }}
    />,
  ))
  const axis = lastYAxis()
  // 旧钳制行为会把 y 轴夹到 18.72~22.88, 曲线全部出界; 现在必须覆盖 15~57.77
  expect(axis.min).toBeLessThanOrEqual(15)
  expect(axis.max).toBeGreaterThanOrEqual(57.77)
})

it('regular stock: adaptive y-axis still clamps to the limit band', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }
  await act(async () => root.render(
    <EChartsIntraday
      data={[
        { datetime: '2026-09-18 09:30:00', open: 20, high: 20, low: 20, close: 20, volume: 100, amount: 200000 },
        { datetime: '2026-09-18 15:00:00', open: 22, high: 22, low: 22, close: 22, volume: 55, amount: 121000 },
      ]}
      date="2026-09-18"
      prevClose={20}
      priceLimit={{ rate: 0.1, limit_up: 22, limit_down: 18, no_limit: false, source: 'rule' }}
    />,
  ))
  const axis = lastYAxis()
  expect(axis.min).toBeCloseTo(18, 6)
  expect(axis.max).toBeCloseTo(22, 6)
})

it('listing day without prevClose anchors y-axis with scale, not zero', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }
  await act(async () => root.render(
    <EChartsIntraday data={wideRows('2026-09-17')} date="2026-09-17" />,
  ))
  const axis = lastYAxis()
  expect(axis.min).toBeUndefined()
  expect(axis.max).toBeUndefined()
  expect(axis.scale).toBe(true)
})

// ================================================================
// 集合竞价: 09:25 竞价柱 + 竞价成交比 (量比) 开关
// ================================================================
const auctionRows = (day: string) => [
  { datetime: `${day} 09:25:00`, open: 119.77, high: 119.77, low: 119.77, close: 119.77, volume: 63, amount: 7544000 },
  { datetime: `${day} 09:31:00`, open: 119.77, high: 119.9, low: 119.5, close: 119.6, volume: 571, amount: 68300000 },
]
const auctionItem = {
  symbol: '600519.SH', open_price: 119.77, open_pct: 0.0056,
  auction_volume: 63, auction_amount: 7544000,
  ratio_volume: 3.52, ratio_amount: 3.4, prev_amount_share: 0.0021,
}

it('集合竞价柱占时轴首格, 交易日刻度后移仍准, 悬停标为集合竞价', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  chart.on.mockImplementation((name, callback) => { chart.handlers[name] = callback })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }
  const onToggle = vi.fn()

  await act(async () => root.render(
    <EChartsIntraday
      data={auctionRows('2026-09-23')}
      date="2026-09-23"
      prevClose={119.1}
      auctionToggle={{ item: auctionItem, baselineDate: '2026-09-22', active: true, onToggle }}
    />,
  ))

  const axis = chart.setOption.mock.calls.at(-1)?.[0]?.xAxis?.[0]
  expect(axis.data[0]).toBe('09:25')
  expect(axis.data.length).toBe(FULL_DAY_TIMES.length + 1)
  expect(axis.axisLabel.formatter('', axis.data.indexOf('09:30'))).toBe('9:30')
  expect(axis.axisLabel.formatter('', axis.data.indexOf('11:30'))).toBe('11:30/13:00')
  expect(axis.axisLabel.formatter('', 0)).toBe('')

  const button = host.querySelector('button')
  expect(button?.textContent).toContain('竞价 3.52×')
  await act(async () => button?.dispatchEvent(new MouseEvent('click', { bubbles: true })))
  expect(onToggle).toHaveBeenCalledTimes(1)

  await act(async () => chart.handlers.updateAxisPointer({ axesInfo: [{ axisDim: 'x', value: 0 }] }))
  expect(host.textContent).toContain('集合竞价')
  expect(host.textContent).not.toContain('09:25 分钟')
})

it('该日拿不到竞价: 按钮禁用显示 —, 时轴退回默认全天', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  const host = document.createElement('div')
  const root = createRoot(host)
  cleanup = async () => { await act(async () => root.unmount()) }

  await act(async () => root.render(
    <EChartsIntraday
      data={[{ datetime: '2026-09-23 09:31:00', open: 119.77, high: 119.9, low: 119.5, close: 119.6, volume: 571, amount: 68300000 }]}
      date="2026-09-23"
      auctionToggle={{ item: null, message: '该标的当日无竞价成交', active: true, onToggle: () => {} }}
    />,
  ))

  const button = host.querySelector('button') as HTMLButtonElement
  expect(button.textContent).toContain('竞价 —')
  expect(button.disabled).toBe(true)
  expect(button.parentElement?.getAttribute('title')).toContain('无竞价成交')
  expect(chart.setOption.mock.calls.at(-1)?.[0]?.xAxis?.[0]?.data.length).toBe(FULL_DAY_TIMES.length)
})
