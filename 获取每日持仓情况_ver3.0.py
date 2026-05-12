# -*- coding: utf-8 -*-
"""
ETF 实盘持仓看板更新脚本

使用方式：
1. 在 Spyder 里按 #%% 分段运行。
2. 每个执行步骤的 cell 里都是直接执行语句，运行该 cell 就会执行对应功能。
3. 直接运行整份脚本会从上到下完成全部流程，并保留过程变量。
"""

#%% 1. 导入库与基础配置
import glob
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime

import pandas as pd


DOWNLOAD_DIR = r"C:\Users\Coeur\Downloads"
TARGET_DIR = r"C:\Users\Coeur\Desktop\红筹投资\组合构建\new_etf_website\etf-portfolio-dashboard"

TARGET_URL = "https://tzzb.10jqka.com.cn/pc/index.html#/myAccount/a/eKkOoy2"
LATEST_FILE_NAME = "latest.xlsx"
DATA_JSON_NAME = "data.json"
NAV_HISTORY_NAME = "nav_history.json"
GIT_REMOTE_NAME = "origin"
GIT_REMOTE_SSH_URL = "git@github.com:Creve-Coeur/etf-dashboard.git"

DEFAULT_BENCHMARK_NAME = "沪深300"
INDEX_HISTORY_START_DATE = "20160101"

CSI_INDEX_DICT = {
    "沪深300": "000300",
    "上证50": "000016",
    "中证800": "000906",
    "中证1000": "000852",
    "中证2000": "932000",
    "中证500": "000905",
    "中证红利": "000922",
    "中证全指": "000985",
}

CNI_INDEX_DICT = {
    "国证2000": "399303",
    "创业板": "399006",
}


#%% 2. 通用清洗函数
def clean_value(value):
    """清理 NaN、pandas 标量和日期，确保可以写入 JSON。"""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            return str(value)
    return value


def clean_dict(record):
    return {key: clean_value(value) for key, value in record.items()}


def clean_records(df):
    df = df.where(pd.notna(df), None)
    return [clean_dict(row) for row in df.to_dict(orient="records")]


def safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_date(value):
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value or "").strip()
    if not text:
        return time.strftime("%Y-%m-%d")
    try:
        return pd.to_datetime(text).strftime("%Y-%m-%d")
    except Exception:
        return text[:10]


def load_json_file(path, fallback):
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback


def get_excel_snapshot_date(excel_path):
    """优先从文件名里的 YYYYMMDD 识别账本日期，识别不到则使用文件修改日期。"""
    file_name = os.path.basename(excel_path)
    match = re.search(r"(20\d{6})", file_name)
    if match:
        return pd.to_datetime(match.group(1), format="%Y%m%d").strftime("%Y-%m-%d")
    return time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(excel_path)))


def get_latest_project_excel():
    """获取项目目录中最新的日期账本文件，排除 latest.xlsx 镜像文件。"""
    pattern = os.path.join(TARGET_DIR, "20*.xlsx")
    files = [path for path in glob.glob(pattern) if os.path.basename(path) != LATEST_FILE_NAME]
    if not files:
        return os.path.join(TARGET_DIR, LATEST_FILE_NAME)
    return max(files, key=os.path.getmtime)


#%% 3. 净值历史维护
def estimate_total_assets(holding_summary):
    """由持仓市值和仓位占比估算总资产，包含现金仓位。"""
    holding_market_value = safe_float(holding_summary.get("持有金额"))
    position_ratio = safe_float(holding_summary.get("仓位占比"))
    if position_ratio > 0:
        return holding_market_value / position_ratio
    return holding_market_value


def update_nav_history(as_of_date, holding_summary):
    """维护真实净值历史。首日净值为 1，之后用每日盈亏滚动计算。"""
    history_path = os.path.join(TARGET_DIR, NAV_HISTORY_NAME)
    history = load_json_file(history_path, {"baseDate": as_of_date, "baseAssets": None, "series": []})

    total_assets = estimate_total_assets(holding_summary)
    holding_market_value = safe_float(holding_summary.get("持有金额"))
    daily_pnl = safe_float(holding_summary.get("当日盈亏"))
    position_ratio = safe_float(holding_summary.get("仓位占比"))

    existing = {item.get("date"): item for item in history.get("series", []) if item.get("date")}
    existing[as_of_date] = {
        "date": as_of_date,
        "totalAssets": round(total_assets, 2),
        "holdingMarketValue": round(holding_market_value, 2),
        "dailyPnL": round(daily_pnl, 2),
        "positionRatio": round(position_ratio, 6),
    }

    series = sorted(existing.values(), key=lambda item: item["date"])
    if not history.get("baseAssets"):
        history["baseDate"] = series[0]["date"] if series else as_of_date
        history["baseAssets"] = series[0]["totalAssets"] if series else total_assets

    previous_nav = 1.0
    previous_assets = safe_float(history.get("baseAssets"))
    for index, item in enumerate(series):
        if index == 0:
            item["nav"] = 1.0
        else:
            daily_return = item["dailyPnL"] / previous_assets if previous_assets else 0
            item["nav"] = round(previous_nav * (1 + daily_return), 6)
        previous_nav = item["nav"]
        previous_assets = safe_float(item.get("totalAssets"))

    history["series"] = series
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2, default=str)

    nav_series = [{"date": item["date"], "nav": item["nav"]} for item in series]
    return nav_series, history


#%% 4. 指数数据获取与检查
def normalize_index_df(df, close_column, base_date=None, end_date=None):
    if df is None or df.empty:
        return pd.DataFrame(columns=["日期", "指数收盘价"])
    df = df[["日期", close_column]].copy()
    df.rename(columns={close_column: "指数收盘价"}, inplace=True)
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    df["指数收盘价"] = df["指数收盘价"].astype(float)
    if base_date:
        df = df[df["日期"] >= base_date]
    if end_date:
        df = df[df["日期"] <= end_date]
    return df.sort_values("日期")


def index_df_to_nav_series(df):
    if df.empty:
        return []
    base_close = safe_float(df.iloc[0]["指数收盘价"])
    if not base_close:
        return []

    series = []
    for _, row in df.iterrows():
        close = safe_float(row["指数收盘价"])
        series.append({"date": row["日期"], "close": round(close, 4), "nav": round(close / base_close, 6)})
    return series


def trim_benchmark_map_to_base(benchmark_map, base_date, end_date):
    """把指数序列裁剪到组合基日之后，并以裁剪后第一条收盘价重新归一化。"""
    base_date = normalize_date(base_date)
    end_date = normalize_date(end_date)
    trimmed_map = {}

    for name, series in (benchmark_map or {}).items():
        filtered = [
            item for item in series
            if base_date <= normalize_date(item.get("date")) <= end_date and safe_float(item.get("close")) > 0
        ]
        if not filtered:
            continue

        base_close = safe_float(filtered[0].get("close"))
        if not base_close:
            continue

        trimmed_map[name] = [
            {
                "date": normalize_date(item.get("date")),
                "close": round(safe_float(item.get("close")), 4),
                "nav": round(safe_float(item.get("close")) / base_close, 6),
            }
            for item in filtered
        ]

    return trimmed_map


def fetch_all_benchmark_series(base_date, end_date):
    """获取全部可选基准指数，并按 base_date 之后第一个交易日归一化。"""
    try:
        import akshare as ak
    except ImportError:
        return {}, {"全部指数": "本机未安装 akshare，暂时无法获取指数数据。"}

    base_date = normalize_date(base_date)
    end_date = normalize_date(end_date)
    end = end_date.replace("-", "")
    benchmark_map = {}
    errors = {}

    for name, symbol in CSI_INDEX_DICT.items():
        try:
            df = ak.stock_zh_index_hist_csindex(symbol=symbol, start_date=INDEX_HISTORY_START_DATE, end_date=end)
            full_index_df = normalize_index_df(df, "收盘")
            index_df = normalize_index_df(df, "收盘", base_date, end_date)
            series = index_df_to_nav_series(index_df)
            if series:
                benchmark_map[name] = series
                print(f"  √ 成功获取: {name} ({symbol}) - 中证接口")
            else:
                latest_date = full_index_df["日期"].max() if not full_index_df.empty else "无数据"
                print(f"  · 暂无更新: {name} ({symbol}) 中证接口最新日期为 {latest_date}，尚无 {base_date} 之后的数据。")
        except Exception as exc:
            errors[name] = f"{name} ({symbol}) 中证接口获取失败：{exc}"
            print(f"  × 获取失败: {errors[name]}")

    for name, symbol in CNI_INDEX_DICT.items():
        try:
            df = ak.index_hist_cni(symbol=symbol, start_date=INDEX_HISTORY_START_DATE, end_date=end)
            full_index_df = normalize_index_df(df, "收盘价")
            index_df = normalize_index_df(df, "收盘价", base_date, end_date)
            series = index_df_to_nav_series(index_df)
            if series:
                benchmark_map[name] = series
                print(f"  √ 成功获取: {name} ({symbol}) - 国证接口")
            else:
                latest_date = full_index_df["日期"].max() if not full_index_df.empty else "无数据"
                print(f"  · 暂无更新: {name} ({symbol}) 国证接口最新日期为 {latest_date}，尚无 {base_date} 之后的数据。")
        except Exception as exc:
            errors[name] = f"{name} ({symbol}) 国证接口获取失败：{exc}"
            print(f"  × 获取失败: {errors[name]}")

    return benchmark_map, errors


def check_benchmark_data(base_date=INDEX_HISTORY_START_DATE, end_date=None, index_name=None):
    """单独检查指数接口。返回 benchmark_map 和 benchmark_errors，方便变量浏览器查看。"""
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print("\n准备工作：正在通过 AkShare 获取基准数据...")
    print(f"查询区间: {normalize_date(base_date)} 至 {normalize_date(end_date)}")
    benchmark_map, benchmark_errors = fetch_all_benchmark_series(base_date, end_date)

    if index_name:
        selected_series = benchmark_map.get(index_name, [])
        selected_error = benchmark_errors.get(index_name)
        print(f"\n指定指数: {index_name}")
        if selected_series:
            print(f"获取成功: {len(selected_series)} 条")
            for row in selected_series[-10:]:
                print(f"{row['date']} 收盘={row['close']} 归一净值={row['nav']}")
        else:
            print(f"获取失败: {selected_error or '没有获取到该指数，请检查名称是否正确。'}")

    print(f"\n成功指数数: {len(benchmark_map)}")
    print(f"失败指数数: {len(benchmark_errors)}")
    return benchmark_map, benchmark_errors


#%% 5. Excel 转网页数据
def build_dashboard_data_from_excel(excel_path, benchmark_map=None, benchmark_errors=None):
    """把最新 Excel 转成网页直接消费的 data.json 结构。"""
    file_generated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(excel_path)))
    site_refreshed_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    xls = pd.ExcelFile(excel_path)
    required_sheets = ["持仓数据", "已清仓", "交易记录"]

    for sheet in required_sheets:
        if sheet not in xls.sheet_names:
            raise ValueError(f"Excel 缺少工作表: {sheet}")

    holdings_df = pd.read_excel(excel_path, sheet_name="持仓数据")
    closed_df = pd.read_excel(excel_path, sheet_name="已清仓")
    transactions_df = pd.read_excel(excel_path, sheet_name="交易记录")

    holdings_df = holdings_df.where(pd.notna(holdings_df), None)
    closed_df = closed_df.where(pd.notna(closed_df), None)
    transactions_df = transactions_df.where(pd.notna(transactions_df), None)

    summary_rows = holdings_df[holdings_df["代码"].astype(str) == "汇总"]
    holding_summary = clean_dict(summary_rows.iloc[0].to_dict()) if not summary_rows.empty else {}
    current_holdings_df = holdings_df[holdings_df["代码"].astype(str) != "汇总"]
    current_holdings = [clean_dict(row) for row in current_holdings_df.to_dict(orient="records")]
    closed_positions = clean_records(closed_df)

    transactions_df = transactions_df.sort_values(["成交日期", "成交时间"], ascending=[False, False], na_position="last")
    transactions = [clean_dict(row) for row in transactions_df.to_dict(orient="records")]

    top_holding = None
    if not current_holdings_df.empty:
        top_holding = current_holdings_df.sort_values("持有金额", ascending=False).iloc[0].to_dict()

    latest_trade_date = transactions[0].get("成交日期") if transactions else None
    trade_date = normalize_date(latest_trade_date) if latest_trade_date else None
    snapshot_date = get_excel_snapshot_date(excel_path)
    as_of_date = max(date for date in [trade_date, snapshot_date] if date)
    nav_series, nav_history = update_nav_history(as_of_date, holding_summary)
    if benchmark_map is None or benchmark_errors is None:
        benchmark_map, benchmark_errors = fetch_all_benchmark_series(nav_history["baseDate"], as_of_date)
    else:
        benchmark_map = trim_benchmark_map_to_base(benchmark_map, nav_history["baseDate"], as_of_date)
    total_assets = estimate_total_assets(holding_summary)

    dashboard_data = {
        "meta": {
            "portfolioName": "ETF实盘持仓账本",
            "manager": "内部投研组",
            "asOfDate": as_of_date,
            "tradeDate": trade_date,
            "snapshotDate": snapshot_date,
            "sourceFile": os.path.basename(excel_path),
            "generatedAt": file_generated_at,
            "siteRefreshedAt": site_refreshed_at,
            "navBaseDate": nav_history["baseDate"],
            "benchmarkName": DEFAULT_BENCHMARK_NAME,
            "benchmarkError": "; ".join(benchmark_errors.values()) if benchmark_errors else None,
            "benchmarkErrors": benchmark_errors,
            "availableBenchmarks": list(benchmark_map.keys()),
        },
        "overview": {
            "totalAssets": round(total_assets, 2),
            "holdingMarketValue": float(holding_summary.get("持有金额") or 0),
            "dailyPnL": float(holding_summary.get("当日盈亏") or 0),
            "dailyPnLRate": float(holding_summary.get("当日盈亏率") or 0),
            "holdingPnL": float(holding_summary.get("持有盈亏") or 0),
            "holdingPnLRate": float(holding_summary.get("持有盈亏率") or 0),
            "weeklyPnL": float(holding_summary.get("本周盈亏") or 0),
            "monthlyPnL": float(holding_summary.get("本月盈亏") or 0),
            "yearlyPnL": float(holding_summary.get("今年盈亏") or 0),
            "positionRatio": float(holding_summary.get("仓位占比") or 0),
            "activeCount": len(current_holdings),
            "closedCount": len(closed_positions),
            "recentTradeCount": sum(1 for row in transactions if row.get("成交日期") == transactions[0].get("成交日期")) if transactions else 0,
            "topHoldingName": top_holding.get("名称") if top_holding else "--",
            "topHoldingWeight": float(top_holding.get("仓位占比") or 0) if top_holding else 0,
        },
        "navSeries": nav_series,
        "benchmarkSeries": benchmark_map.get(DEFAULT_BENCHMARK_NAME, []),
        "benchmarkSeriesMap": benchmark_map,
        "sheets": {
            "currentHoldings": current_holdings,
            "holdingSummary": holding_summary,
            "closedPositions": closed_positions,
            "transactions": transactions[:120],
        },
        "strategy": {
            "title": "自动同步 Excel 展示",
            "positioning": "页面数据由最新导出的 Excel 自动生成，无需再手工维护持仓、清仓和交易记录。净值曲线来自每日真实资产快照，并可选择多个指数作为同步基日对比。",
            "riskRules": [
                "每日运行下载脚本后，网页会同步刷新为最新账本数据。",
                "持仓明细、已清仓和交易记录全部来自 Excel 原始工作表。",
                "组合净值以首次记录日为基日，后续每日更新同一份净值历史。",
            ],
        },
    }
    return dashboard_data


def refresh_data_json_from_excel(excel_path=None, benchmark_map=None, benchmark_errors=None):
    """根据最新 Excel 刷新网页数据文件。返回 dashboard_data 方便变量浏览器查看。"""
    if excel_path is None:
        excel_path = get_latest_project_excel()
    target_json_path = os.path.join(TARGET_DIR, DATA_JSON_NAME)
    dashboard_data = build_dashboard_data_from_excel(excel_path, benchmark_map=benchmark_map, benchmark_errors=benchmark_errors)

    with open(target_json_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2, default=str)

    print(f"网页数据已刷新: {target_json_path}")
    print(f"组合净值记录数: {len(dashboard_data['navSeries'])}")
    print(f"可用对比指数数: {len(dashboard_data['benchmarkSeriesMap'])}")
    for name, series in dashboard_data["benchmarkSeriesMap"].items():
        print(f"  {name}: {len(series)} 条")
    if dashboard_data["meta"].get("benchmarkError"):
        print(dashboard_data["meta"]["benchmarkError"])
    return dashboard_data


#%% 6. 下载文件搬运
def get_latest_downloaded_file(download_dir):
    """获取下载目录中最新生成的 xlsx 或 csv 文件。"""
    files = glob.glob(os.path.join(download_dir, "*.xlsx")) + glob.glob(os.path.join(download_dir, "*.csv"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def open_broker_website():
    """只打开券商网站，不做后续处理。"""
    start_time = time.time()
    os.system(f'start chrome "{TARGET_URL}"')
    print("已打开券商网站。start_time 已返回，可用于监控新下载文件。")
    return start_time


def wait_for_new_export(start_time, max_wait_time=20):
    """等待券商网站导出的新文件下载完成。"""
    elapsed_time = 0
    while elapsed_time < max_wait_time:
        if glob.glob(os.path.join(DOWNLOAD_DIR, "*.crdownload")):
            time.sleep(1)
            elapsed_time += 1
            continue

        latest_file = get_latest_downloaded_file(DOWNLOAD_DIR)
        if latest_file and os.path.getmtime(latest_file) > start_time:
            return latest_file

        time.sleep(1)
        elapsed_time += 1
    return None


def move_export_to_project(found_file):
    """把下载文件移动到项目目录，并刷新 latest.xlsx。"""
    file_name = os.path.basename(found_file)
    file_mtime = os.path.getmtime(found_file)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(file_mtime))

    name_part, ext_part = os.path.splitext(file_name)
    clean_name = re.sub(r"\s*\(\d+\)", "", name_part)
    today_str = time.strftime("%Y%m%d")
    new_file_name = f"{today_str}_{clean_name}{ext_part}"

    final_target_path = os.path.join(TARGET_DIR, new_file_name)
    latest_target_path = os.path.join(TARGET_DIR, LATEST_FILE_NAME)

    print(f"捕获文件: {file_name}")
    print(f"文件生成时间: {formatted_time}")
    shutil.move(found_file, final_target_path)

    if ext_part.lower() == ".xlsx":
        shutil.copy2(final_target_path, latest_target_path)
        print(f"latest.xlsx 已刷新: {latest_target_path}")
        return final_target_path
    return final_target_path


#%% 7. 云端部署
def run_git_command(args):
    """执行 Git 命令，返回 result 方便变量浏览器检查 stdout/stderr。"""
    result = subprocess.run(
        args,
        cwd=TARGET_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    print(f"\n$ {' '.join(args)}")
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result


def ensure_ssh_remote():
    """确保 Git remote 使用 SSH 地址。"""
    current_result = run_git_command(["git", "remote", "get-url", GIT_REMOTE_NAME])
    current_url = current_result.stdout.strip()
    if current_url == GIT_REMOTE_SSH_URL:
        print(f"Git remote 已是 SSH: {GIT_REMOTE_SSH_URL}")
        return current_result

    print(f"正在将 Git remote 切换为 SSH: {GIT_REMOTE_SSH_URL}")
    set_result = run_git_command(["git", "remote", "set-url", GIT_REMOTE_NAME, GIT_REMOTE_SSH_URL])
    if set_result.returncode == 0:
        print("Git remote 已切换为 SSH。")
    else:
        print("Git remote 切换失败，请检查 set_result.stdout 和 set_result.stderr。")
    return set_result


def commit_local_changes():
    """提交本地更新。没有新变化时也视为正常。"""
    add_result = run_git_command(["git", "add", "."])
    commit_result = run_git_command(["git", "commit", "-m", "Auto-update daily portfolio data"])

    no_changes = "nothing to commit" in commit_result.stdout or "working tree clean" in commit_result.stdout
    commit_ok = commit_result.returncode == 0 or no_changes
    if commit_ok:
        print("本地提交步骤完成。")
    else:
        print("本地提交失败，请检查 commit_result.stdout 和 commit_result.stderr。")

    return {
        "add_result": add_result,
        "commit_result": commit_result,
        "commit_ok": commit_ok,
    }


def push_to_cloud(ensure_remote=True):
    """只推送到云端。网络失败后可以单独重跑这个函数。"""
    if ensure_remote:
        remote_result = ensure_ssh_remote()
        if remote_result.returncode != 0:
            print("未能确认 SSH remote，已停止推送。")
            return remote_result

    push_result = run_git_command(["git", "push", "-u", GIT_REMOTE_NAME, "main"])
    if push_result.returncode == 0:
        print("云端推送成功。")
    else:
        print("云端推送失败，请检查 push_result.stdout 和 push_result.stderr。")
    return push_result

def deploy_to_cloud():
    """提交本地更新并推送云端。返回每一步结果，方便变量浏览器检查。"""
    remote_result = ensure_ssh_remote()
    commit_info = commit_local_changes()
    push_result = None
    if remote_result.returncode == 0 and commit_info["commit_ok"]:
        push_result = push_to_cloud(ensure_remote=False)

    deploy_ok = remote_result.returncode == 0 and commit_info["commit_ok"] and push_result is not None and push_result.returncode == 0
    if deploy_ok:
        print("云端部署成功。")
    else:
        print("云端部署未完成。若只是 GitHub 连接失败，网络恢复后单独运行：push_result = push_to_cloud()")

    return {
        "remote_result": remote_result,
        "commit_info": commit_info,
        "push_result": push_result,
        "deploy_ok": deploy_ok,
    }


#%% 8. 可选：完整流程函数
def run_full_workflow(max_wait_time=20, deploy=True):
    """完整流程函数。直接运行脚本时会自动调用，也可以手动分步调用。"""
    os.makedirs(TARGET_DIR, exist_ok=True)
    start_time = open_broker_website()

    print(f"正在监控下载目录: {DOWNLOAD_DIR}")
    found_file = wait_for_new_export(start_time, max_wait_time=max_wait_time)
    if not found_file:
        print("任务超时，未检测到新下载的账本文件。")
        return None

    excel_path = move_export_to_project(found_file)
    dashboard_data = None
    deploy_result = None
    if excel_path.lower().endswith(".xlsx"):
        dashboard_data = refresh_data_json_from_excel(excel_path)
    if deploy:
        deploy_result = deploy_to_cloud()

    return {
        "excel_path": excel_path,
        "dashboard_data": dashboard_data,
        "deploy_result": deploy_result,
    }


#%% 9. 分步骤执行区：运行到哪个 cell，就直接执行哪个步骤
# 说明：
# - 不需要取消注释，也不需要设置触发开关。
# - 在 Spyder 里单独运行某个 cell，就会执行该 cell 的功能。
# - 直接运行整份脚本，会按下面顺序完整执行。
# - 下面产生的 benchmark_map、start_time、found_file、excel_path、dashboard_data、deploy_result 都会出现在变量浏览器。

#%% 9.1 获取并检查全部指数
benchmark_map, benchmark_errors = check_benchmark_data()


#%% 9.2 打开券商网站并记录开始时间
start_time = open_broker_website()


#%% 9.3 等待并捕获新下载文件
WAIT_MAX_SECONDS = 20
found_file = wait_for_new_export(start_time, max_wait_time=WAIT_MAX_SECONDS)


#%% 9.4 移动下载文件并刷新 latest.xlsx
if found_file is None:
    raise RuntimeError("未检测到新下载的账本文件，请检查券商网站是否已成功导出。")
excel_path = move_export_to_project(found_file)


#%% 9.5 根据 latest.xlsx 刷新网页数据
dashboard_data = refresh_data_json_from_excel(excel_path, benchmark_map=benchmark_map, benchmark_errors=benchmark_errors)


#%% 9.6 执行云端部署
deploy_result = deploy_to_cloud()


#%% 9.7 汇总本次执行结果
workflow_result = {
    "benchmark_map": benchmark_map,
    "benchmark_errors": benchmark_errors,
    "start_time": start_time,
    "found_file": found_file,
    "excel_path": excel_path,
    "dashboard_data": dashboard_data,
    "deploy_result": deploy_result,
}
