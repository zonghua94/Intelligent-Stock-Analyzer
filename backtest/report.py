# -*- coding: utf-8 -*-

import os
import pandas as pd
from datetime import datetime
from typing import Optional

from utils.logger import Logger
from .config import BacktestArgs
from .engine import BacktestResult

logger = Logger(__name__)


class BacktestReporter:
    def __init__(self, result: BacktestResult, args: BacktestArgs):
        self.result = result
        self.args = args

    def generate_markdown_report(self) -> str:
        r = self.result
        m = r.metrics

        lines = [
            "# 回测报告",
            "",
            f"> 回测区间: {self.args.start_date} ~ {self.args.end_date or '今天'}",
            f"> 回测股票: {len(self.args.stock_codes)} 只 | "
            f"产生信号: {r.total_signals} 个 | "
            f"涉及股票: {m.get('unique_stocks', 0)} 只 | "
            f"月均信号: {m.get('avg_signals_per_month', 'N/A')}",
            "",
            "---",
            "",
        ]

        if not r.stock_results:
            lines.append("**未产生任何买入信号。**")
            return "\n".join(lines)

        fwd = self.args.forward_return_days
        lines.extend([
            "## 核心指标",
            "",
            "| 指标 | " + " | ".join(f"{n}日" for n in fwd) + " |",
            "|------|" + "|".join("------" for _ in fwd) + "|",
        ])

        metric_rows = [
            ("win_rate", "胜率(%)"),
            ("avg_return", "平均收益(%)"),
            ("median_return", "中位收益(%)"),
            ("sharpe", "夏普比率"),
            ("profit_factor", "盈亏比"),
            ("max_drawdown", "最大回撤(%)"),
            ("cumulative_return", "累计收益(%)"),
            ("max_consecutive_loss", "最大连亏"),
        ]
        for key, label in metric_rows:
            row = f"| {label} |"
            for n in fwd:
                val = m.get(f"{key}_{n}d", "N/A")
                if isinstance(val, float):
                    if val == float('inf'):
                        row += " +inf |"
                    else:
                        row += f" {val:.2f} |"
                else:
                    row += f" {val} |"
            lines.append(row)

        lines.extend(["", "---", ""])

        lines.extend([
            "## 收益分布",
            "",
        ])
        if not r.summary_df.empty and f'forward_{fwd[0]}d' in r.summary_df.columns:
            first_col = f'forward_{fwd[0]}d'
            returns = r.summary_df[first_col].dropna()
            if len(returns) > 0:
                bins = [
                    ("< -5%", (returns < -5).sum()),
                    ("-5% ~ -2%", ((returns >= -5) & (returns < -2)).sum()),
                    ("-2% ~ 0%", ((returns >= -2) & (returns < 0)).sum()),
                    ("0% ~ 2%", ((returns >= 0) & (returns < 2)).sum()),
                    ("2% ~ 5%", ((returns >= 2) & (returns < 5)).sum()),
                    ("5% ~ 10%", ((returns >= 5) & (returns < 10)).sum()),
                    ("> 10%", (returns >= 10).sum()),
                ]
                lines.append(f"**{fwd[0]}日收益分布:**")
                lines.append("")
                lines.append("| 区间 | 信号数 | 占比 |")
                lines.append("|------|-------|------|")
                for label, count in bins:
                    pct = count / len(returns) * 100
                    lines.append(f"| {label} | {count} | {pct:.1f}% |")
                lines.extend(["", "---", ""])

        lines.extend([
            "## 个股信号统计",
            "",
            "| 股票代码 | 信号数 | " + " | ".join(f"{n}日均收益(%)" for n in fwd) + " | " + f"{fwd[0]}日胜率(%) |",
            "|---------|-------|" + "|".join("---------" for _ in fwd) + "|----------|",
        ])
        sorted_results = sorted(r.stock_results, key=lambda x: x.signal_count, reverse=True)
        for sr in sorted_results:
            if sr.signal_count == 0:
                continue
            row = f"| {sr.stock_code} | {sr.signal_count} |"
            for n in fwd:
                col = f'forward_{n}d'
                if col in sr.signals_df.columns:
                    avg = sr.signals_df[col].mean()
                    row += f" {avg:.2f} |"
                else:
                    row += " N/A |"
            first_col = f'forward_{fwd[0]}d'
            if first_col in sr.signals_df.columns:
                wr = (sr.signals_df[first_col].dropna() > 0).mean() * 100
                row += f" {wr:.1f} |"
            else:
                row += " N/A |"
            lines.append(row)

        lines.extend([
            "",
            "---",
            "",
            f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        ])

        return "\n".join(lines)

    def generate_chart(self, output_path: str):
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
            matplotlib.rcParams['axes.unicode_minus'] = False
        except ImportError:
            logger.warning("matplotlib 未安装，跳过图表生成")
            return

        r = self.result
        if r.summary_df.empty:
            return

        fwd = self.args.forward_return_days
        first_n = fwd[0]
        col = f'forward_{first_n}d'

        if col not in r.summary_df.columns:
            return

        df = r.summary_df.dropna(subset=[col]).copy()
        if df.empty:
            return

        fig, axes = plt.subplots(3, 1, figsize=(14, 12))

        # 1. 信号时间分布散点图
        ax1 = axes[0]
        colors = df[col].apply(lambda x: 'green' if x > 0 else 'red')
        ax1.scatter(df['date'], df[col], c=colors, alpha=0.6, s=20)
        ax1.axhline(y=0, color='gray', linestyle='--', linewidth=0.8)
        ax1.set_title(f'{first_n}日前向收益时间分布')
        ax1.set_ylabel('收益率(%)')
        ax1.grid(True, alpha=0.3)

        # 2. 收益分布直方图
        ax2 = axes[1]
        for n in fwd:
            c = f'forward_{n}d'
            if c in r.summary_df.columns:
                data = r.summary_df[c].dropna()
                if len(data) > 0:
                    ax2.hist(data, bins=30, alpha=0.5, label=f'{n}日')
        ax2.axvline(x=0, color='gray', linestyle='--', linewidth=0.8)
        ax2.set_title('收益率分布')
        ax2.set_xlabel('收益率(%)')
        ax2.set_ylabel('信号数')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # 3. 月度信号数柱状图
        ax3 = axes[2]
        if 'date' in df.columns:
            monthly = df.set_index('date').resample('ME').size()
            if len(monthly) > 0:
                ax3.bar(monthly.index, monthly.values, width=20, alpha=0.7, color='steelblue')
                ax3.set_title('月度信号数量')
                ax3.set_ylabel('信号数')
                ax3.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        logger.info(f"图表已保存: {output_path}")

    def save_report(self):
        os.makedirs(self.args.output_dir, exist_ok=True)

        report = self.generate_markdown_report()
        report_path = os.path.join(self.args.output_dir, "backtest_report.md")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        logger.info(f"报告已保存: {report_path}")

        if not self.result.summary_df.empty:
            csv_path = os.path.join(self.args.output_dir, "signals.csv")
            self.result.summary_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"信号明细已保存: {csv_path}")

        if self.args.enable_chart and not self.result.summary_df.empty:
            chart_path = os.path.join(self.args.output_dir, "backtest_chart.png")
            self.generate_chart(chart_path)
