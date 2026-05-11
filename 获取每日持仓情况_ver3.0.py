import os
import time
import glob
import shutil
import re
import json
import subprocess  # 🌟 必须导入这个库，才能让 Python 执行 Git 命令
import pandas as pd
import mimetypes 

# ================= 1. 核心路径配置 =================

# 你的 Chrome 浏览器默认下载目录 (通常是 C:\Users\你的用户名\Downloads)
DOWNLOAD_DIR = r"C:\Users\Coeur\Downloads"

# 你的量化实盘项目文件夹 (最终要把文件移到这里)
TARGET_DIR = r"C:\Users\Coeur\Desktop\红筹投资\组合构建\new_etf_website\etf-portfolio-dashboard"

# 同花顺投资账本网址
TARGET_URL = "https://tzzb.10jqka.com.cn/pc/index.html#/myAccount/a/eKkOoy2"
LATEST_FILE_NAME = "latest.xlsx"
DATA_JSON_NAME = "data.json"

# ===================================================

def get_latest_downloaded_file(download_dir):
    """获取下载目录中最新生成的 xlsx 或 csv 文件"""
    list_of_files = glob.glob(os.path.join(download_dir, "*.xlsx")) + glob.glob(os.path.join(download_dir, "*.csv"))
    if not list_of_files:
        return None
    latest_file = max(list_of_files, key=os.path.getmtime)
    return latest_file

def clean_records(df):
    """将 DataFrame 转成适合 JSON 序列化的记录列表。"""
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")

def clean_value(value):
    """把 NaN / pandas 标量安全转成合法 JSON 值。"""
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
    """清洗单条字典记录，确保没有 NaN。"""
    return {key: clean_value(value) for key, value in record.items()}

def build_dashboard_data_from_excel(excel_path):
    """把最新 Excel 转成网页直接消费的 data.json 结构。"""
    file_generated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(excel_path)))
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
    closed_positions = [clean_dict(row) for row in closed_positions]

    top_holding = None
    if not current_holdings_df.empty:
        top_holding = current_holdings_df.sort_values("持有金额", ascending=False).iloc[0].to_dict()

    latest_trade_date = None
    if transactions:
        latest_trade_date = transactions[0].get("成交日期")
        if hasattr(latest_trade_date, "strftime"):
            latest_trade_date = latest_trade_date.strftime("%Y-%m-%d")

    data = {
        "meta": {
            "portfolioName": "ETF实盘持仓账本",
            "manager": "内部投研组",
            "asOfDate": latest_trade_date or time.strftime("%Y-%m-%d"),
            "sourceFile": os.path.basename(excel_path),
            "generatedAt": file_generated_at
        },
        "overview": {
            "totalAssets": float(holding_summary.get("持有金额") or 0),
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
            "topHoldingWeight": float(top_holding.get("仓位占比") or 0) if top_holding else 0
        },
        "navSeries": [
            {"date": "2026-04-14", "nav": 1.0},
            {"date": "2026-04-15", "nav": 0.9988},
            {"date": "2026-04-16", "nav": 1.0012},
            {"date": "2026-04-17", "nav": 1.0031},
            {"date": "2026-04-18", "nav": 1.0047},
            {"date": "2026-04-21", "nav": 1.0068},
            {"date": "2026-04-22", "nav": 1.0055},
            {"date": "2026-04-23", "nav": 1.008},
            {"date": "2026-04-24", "nav": 1.0101},
            {"date": "2026-04-25", "nav": 1.0115},
            {"date": "2026-04-28", "nav": 1.0093}
        ],
        "sheets": {
            "currentHoldings": current_holdings,
            "holdingSummary": holding_summary,
            "closedPositions": closed_positions,
            "transactions": transactions[:120]
        },
        "strategy": {
            "title": "自动同步 Excel 展示",
            "positioning": "页面数据由最新导出的 Excel 自动生成，无需再手工维护持仓、清仓和交易记录。当前净值曲线仍保留为独立字段，便于后续接入历史净值序列。",
            "riskRules": [
                "每日运行下载脚本后，网页会同步刷新为最新账本数据。",
                "持仓明细、已清仓和交易记录全部来自 Excel 原始工作表。",
                "若要继续增强净值曲线，可后续追加历史归档文件自动汇总逻辑。"
            ]
        }
    }
    return data

def refresh_data_json_from_excel(excel_path):
    """根据最新 Excel 刷新网页的数据文件。"""
    target_json_path = os.path.join(TARGET_DIR, DATA_JSON_NAME)
    data = build_dashboard_data_from_excel(excel_path)
    with open(target_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"🧾 网页数据已刷新：\n📁 {target_json_path}")

def run_hybrid_crawler():
    if not os.path.exists(TARGET_DIR):
        os.makedirs(TARGET_DIR)

    start_time = time.time()

    print(f"🚀 [第一步] Python 正在召唤 Chrome 浏览器...")
    print(f"   (Automa 插件检测到网址后将自动执行导出并关闭网页)")
    
    os.system(f'start chrome "{TARGET_URL}"')

    print(f"⏳ [第二步] 雷达开启：监控下载文件夹 ({DOWNLOAD_DIR})...")
    
    max_wait_time = 20  
    elapsed_time = 0
    found_file = None

    while elapsed_time < max_wait_time:
        cr_downloads = glob.glob(os.path.join(DOWNLOAD_DIR, "*.crdownload"))
        if cr_downloads:
            time.sleep(1)
            elapsed_time += 1
            continue

        latest_file = get_latest_downloaded_file(DOWNLOAD_DIR)
        
        if latest_file and os.path.getmtime(latest_file) > start_time:
            found_file = latest_file
            break 

        time.sleep(1)
        elapsed_time += 1

    # ================= 3. 文件转移与重命名 =================
    if found_file:
        file_name = os.path.basename(found_file)
        
        file_mtime = os.path.getmtime(found_file)
        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(file_mtime))
        
        name_part, ext_part = os.path.splitext(file_name)
        clean_name = re.sub(r'\s*\(\d+\)', '', name_part)  
        
        today_str = time.strftime("%Y%m%d")
        new_file_name = f"{today_str}_{clean_name}{ext_part}"  
        
        final_target_path = os.path.join(TARGET_DIR, new_file_name)
        latest_target_path = os.path.join(TARGET_DIR, LATEST_FILE_NAME)
        
        print(f"\n📦 [第三步] 捕获猎物: {file_name}")
        print(f"🕒 文件真实生成时间: {formatted_time}") 
        print(f"🚚 正在进行空间转移并清洗文件名...")
        
        try:
            shutil.move(found_file, final_target_path)

            if ext_part.lower() == ".xlsx":
                shutil.copy2(final_target_path, latest_target_path)
                print(f"🆕 最新账单镜像已刷新：\n📁 {latest_target_path}")
                refresh_data_json_from_excel(latest_target_path)

            print(f"🎉 文件处理结束！今日账单已入库：\n📁 {final_target_path}")
            
            # ================= 4. 自动部署到 GitHub =================
            print("\n☁️ [第四步] 正在将最新数据一键同步到 GitHub 云端...")
            
            # 自动化 Git 命令：打包 -> 贴标签 -> 推送
            deploy_command = 'git add . && git commit -m "Auto-update daily portfolio data" && git push'
            
            result = subprocess.run(
                deploy_command, 
                cwd=TARGET_DIR,  
                capture_output=True, 
                text=True,
                encoding="utf-8",
                errors="ignore",
                shell=True
            )
            
            # 判断是否推送成功，或者数据没有变化（working tree clean）
            if result.returncode == 0 or "working tree clean" in result.stdout:
                print("\n✅ 网页更新成功！你的手机端约 1~2 分钟后即可看到最新数据！")
                print("🔗 你的专属看板网址: https://creve-coeur.github.io/etf-dashboard/")
            else:
                print("\n❌ 云端部署失败，请检查报错信息：")
                print(result.stderr or result.stdout)
                
        # 🌟 捕获异常，防止报错崩溃
        except PermissionError:
            print(f"❌ 移动失败，文件可能被其他程序占用: {found_file}")
        except Exception as e:
            print(f"❌ 部署过程中发生未知错误: {e}")
            
    # 🌟 这里的 else 对应 if found_file:
    else:
        print("\n❌ 任务超时 (20秒)！未能在下载文件夹中检测到新产生的数据文件。")

if __name__ == "__main__":
    run_hybrid_crawler()