import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
import re
from difflib import SequenceMatcher
import time

class EDINETCompanyExtractor:
    def __init__(self, subscription_key):
        """
        EDINET企業データ抽出クライアントの初期化（API v2対応）
        """
        self.subscription_key = subscription_key
        self.base_url = "https://api.edinet-fsa.go.jp/api/v2"
        self.headers = {
            "Content-Type": "application/json"
        }
        
        # 指定されたカラムの財務指標マッピング
        self.target_indicators = {
            "売上高": ["netsales", "operatingrevenues", "revenue", "operatingincome"],
            "資本金": ["capitalstock", "paidincapital", "capital"],
            "従業員数": ["numberofemployees", "employees"]
        }
    
    def get_securities_reports_by_date_range(self, search_days=730):
        """
        指定期間の有価証券報告書のみを取得
        
        Args:
            search_days (int): 検索する過去の日数（デフォルト730日）
        
        Returns:
            list: 有価証券報告書のデータリスト
        """
        # 現在日時を取得（最新データを確実に取得するため）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=search_days)
        
        print(f"🔍 有価証券報告書検索期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}")
        print(f"   検索対象: 有価証券報告書（docTypeCode: 120）のみ")
        
        securities_reports = []
        current_date = start_date
        request_count = 0
        days_with_reports = 0
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            try:
                # API制限対策で少し待機
                if request_count > 0 and request_count % 15 == 0:
                    print(f"  💤 API制限対策で3秒待機... ({request_count}日処理済み)")
                    time.sleep(3)
                
                documents = self._get_securities_reports_by_date(date_str)
                if documents:
                    securities_reports.extend(documents)
                    days_with_reports += 1
                    print(f"  📅 {date_str}: {len(documents)}件の有価証券報告書")
                
                request_count += 1
                current_date += timedelta(days=1)
                
                # 進捗表示（週単位）
                if request_count % 7 == 0:
                    progress = (request_count / search_days) * 100
                    print(f"  📊 進捗: {progress:.1f}% ({request_count}/{search_days}日)")
                
            except Exception as e:
                print(f"  ❌ {date_str} の取得でエラー: {str(e)}")
                current_date += timedelta(days=1)
                continue
        
        print(f"✅ 検索完了: {days_with_reports}日間で合計 {len(securities_reports)} 件の有価証券報告書を取得")
        return securities_reports
    
    def _get_securities_reports_by_date(self, date_str):
        """
        指定日の有価証券報告書のみを取得
        """
        url = f"{self.base_url}/documents.json"
        
        params = {
            "date": date_str,
            "type": "2",  # 提出書類一覧およびメタデータを取得
            "Subscription-Key": self.subscription_key
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            securities_reports = []
            if "results" in data and data["results"]:
                for result in data["results"]:
                    # 有価証券報告書（docTypeCode: 120）のみをフィルタリング
                    if result.get("docTypeCode") == "120":
                        # 証券コードまたはEDINETコードがある企業のみ（上場企業等）
                        if result.get("secCode") or result.get("edinetCode"):
                            securities_reports.append(result)
                
            return securities_reports
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")
    
    def find_latest_company_reports(self, company_names, securities_reports):
        """
        企業名リストに該当する最新の有価証券報告書を検索
        
        Args:
            company_names (list): 検索する企業名のリスト
            securities_reports (list): 検索対象の有価証券報告書データ
        
        Returns:
            dict: 企業名をキーとした最新報告書データの辞書
        """
        company_reports = {}
        
        print(f"\\n🔍 {len(company_names)} 社の最新有価証券報告書を検索中...")
        
        for company_name in company_names:
            print(f"📊 {company_name} の最新報告書を検索中...")
            
            company_matches = []
            
            # 全有価証券報告書から企業名にマッチするものを検索
            for report in securities_reports:
                filer_name = report.get("filerName", "")
                if not filer_name:
                    continue
                
                # 類似度計算
                score = SequenceMatcher(None, company_name.lower(), filer_name.lower()).ratio()
                
                if score >= 0.5:  # 50%以上の類似度があるもの
                    company_matches.append({
                        "report": report,
                        "score": score,
                        "filer_name": filer_name,
                        "submit_date": report.get("submitDateTime", "")
                    })
            
            if company_matches:
                # 類似度順でソート、同じ企業の場合は提出日順でソート
                company_matches.sort(key=lambda x: (x["score"], x["submit_date"]), reverse=True)
                
                # 最高類似度のグループを取得
                best_score = company_matches[0]["score"]
                best_matches = [m for m in company_matches if m["score"] == best_score]
                
                # 同じ企業の場合は最新の報告書を選択
                latest_match = max(best_matches, key=lambda x: x["submit_date"])
                
                company_reports[company_name] = {
                    "report": latest_match["report"],
                    "actual_name": latest_match["filer_name"],
                    "similarity": latest_match["score"],
                    "submit_date": latest_match["submit_date"],
                    "alternatives_count": len(company_matches) - 1
                }
                
                submit_date = latest_match["submit_date"][:10] if latest_match["submit_date"] else "不明"
                print(f"  ✅ 発見: {latest_match['filer_name']}")
                print(f"     類似度: {latest_match['score']:.2%} | 提出日: {submit_date}")
                if len(company_matches) > 1:
                    print(f"     他に {len(company_matches)-1} 件の候補報告書がありました")
            else:
                company_reports[company_name] = {
                    "report": None,
                    "actual_name": "",
                    "similarity": 0,
                    "error": f"類似度50%以上の企業が見つかりませんでした",
                    "alternatives_count": 0
                }
                print(f"  ❌ 見つからず: 該当する企業の有価証券報告書が見つかりません")
        
        return company_reports
    
    def get_xbrl_document(self, doc_id):
        """XBRLドキュメントを取得"""
        url = f"{self.base_url}/documents/{doc_id}"
        
        params = {
            "type": "5",  # CSV形式のXBRLデータ
            "Subscription-Key": self.subscription_key
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            return {
                "success": True,
                "content": response.content
            }
            
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"XBRLドキュメント取得エラー: {str(e)}"
            }
    
    def extract_financial_data_from_csv(self, csv_content):
        """
        CSVコンテンツから指定された財務データを抽出
        """
        try:
            csv_text = csv_content.decode('utf-8')
            lines = csv_text.strip().split('\\n')
            
            if len(lines) < 2:
                return {"success": False, "error": "CSVデータが不正です"}
            
            # CSVをパース
            data_rows = []
            for line in lines:
                # CSVの行を適切に分割（引用符内のカンマを考慮）
                row = []
                current_field = ""
                in_quotes = False
                
                for char in line:
                    if char == '"':
                        in_quotes = not in_quotes
                    elif char == ',' and not in_quotes:
                        row.append(current_field.strip('"'))
                        current_field = ""
                    else:
                        current_field += char
                
                # 最後のフィールドを追加
                row.append(current_field.strip('"'))
                
                if len(row) >= 6:
                    data_rows.append(row)
            
            if not data_rows:
                return {"success": False, "error": "有効なデータ行が見つかりません"}
            
            headers = data_rows[0] if data_rows else []
            data_dict = {}
            subsidiary_info = []
            
            # 各行からデータを抽出
            for row in data_rows[1:]:
                if len(row) >= len(headers):
                    row_dict = dict(zip(headers, row))
                    
                    element_name = row_dict.get("要素名", "").lower()
                    context_ref = row_dict.get("コンテキストRef", "")
                    value = row_dict.get("値", "")
                    
                    # 当期のデータのみを対象
                    if "prior" not in context_ref.lower() and value and value != "-":
                        
                        # 各指標について検索
                        for indicator_name, keywords in self.target_indicators.items():
                            for keyword in keywords:
                                if keyword in element_name:
                                    # より具体的なマッチを優先
                                    if indicator_name not in data_dict or len(keyword) > len(data_dict.get(f"{indicator_name}_keyword", "")):
                                        cleaned_value = self._clean_numeric_value(value)
                                        if cleaned_value is not None:
                                            data_dict[indicator_name] = cleaned_value
                                            data_dict[f"{indicator_name}_keyword"] = keyword
                                    break
                        
                        # グループ企業情報の抽出
                        if any(term in element_name for term in ["subsidiary", "affiliate", "関係会社", "子会社"]):
                            if isinstance(value, str) and len(value) > 3:
                                subsidiary_info.append(value)
            
            # 内部キーワード情報を削除
            data_dict = {k: v for k, v in data_dict.items() if not k.endswith('_keyword')}
            
            # グループ企業情報をまとめる
            if subsidiary_info:
                data_dict["関連企業情報"] = "; ".join(set(subsidiary_info[:5]))
            
            return {
                "success": True,
                "data": data_dict
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"財務データ抽出エラー: {str(e)}"
            }
    
    def _clean_numeric_value(self, value_str):
        """数値文字列をクリーンアップ"""
        try:
            # カンマや円マークを除去
            cleaned = re.sub(r'[,¥円]', '', str(value_str))
            # 数値以外の文字を除去（マイナス記号と小数点は保持）
            cleaned = re.sub(r'[^\\d.-]', '', cleaned)
            
            if cleaned and cleaned != '-':
                return float(cleaned)
            return None
        except:
            return None
    
    def extract_companies_data(self, company_names, search_days=730):
        """
        複数企業名の最新有価証券報告書データを抽出
        
        Args:
            company_names (list): 企業名のリスト
            search_days (int): 検索する過去の日数（デフォルト730日）
        
        Returns:
            pd.DataFrame: 抽出結果のDataFrame
        """
        print(f"=== EDINET 最新有価証券報告書データ抽出 ===")
        print(f"対象企業数: {len(company_names)}")
        print(f"検索期間: 過去{search_days}日（最新データまで）")
        print("=" * 60)
        
        # 1. 指定期間の有価証券報告書のみを取得
        securities_reports = self.get_securities_reports_by_date_range(search_days)
        
        if not securities_reports:
            print("❌ 期間内に有価証券報告書が見つかりませんでした")
            return pd.DataFrame()
        
        # 2. 企業名で最新の有価証券報告書を検索
        company_reports = self.find_latest_company_reports(company_names, securities_reports)
        
        # 3. 各企業のデータを抽出
        results = []
        
        print(f"\\n=== 財務データ抽出処理 ===")
        
        for i, company_name in enumerate(company_names, 1):
            print(f"\\n[{i}/{len(company_names)}] {company_name} の財務データ抽出中...")
            
            company_info = company_reports.get(company_name)
            
            if not company_info or not company_info.get("report"):
                error_msg = company_info.get("error", "不明なエラー") if company_info else "企業情報が見つかりません"
                print(f"  ❌ エラー: {error_msg}")
                results.append({
                    "企業名": company_name,
                    "docID": None,
                    "docDescription": None,
                    "提出日": None,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": error_msg
                })
                continue
            
            report = company_info["report"]
            actual_name = company_info["actual_name"]
            doc_id = report.get("docID")
            doc_description = report.get("docDescription", "")
            submit_date = report.get("submitDateTime", "")[:10] if report.get("submitDateTime") else ""
            
            print(f"  📊 企業名: {actual_name}")
            print(f"  📄 報告書: {doc_description}")
            print(f"  📅 提出日: {submit_date}")
            print(f"  🆔 docID: {doc_id}")
            
            # XBRLデータを取得
            print(f"  🔄 XBRLデータを取得中...")
            xbrl_result = self.get_xbrl_document(doc_id)
            
            if not xbrl_result["success"]:
                print(f"  ❌ XBRLデータ取得エラー: {xbrl_result['error']}")
                results.append({
                    "企業名": actual_name,
                    "docID": doc_id,
                    "docDescription": doc_description,
                    "提出日": submit_date,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": xbrl_result["error"]
                })
                continue
            
            # 財務データを抽出
            print(f"  🔄 財務データを解析中...")
            financial_result = self.extract_financial_data_from_csv(xbrl_result["content"])
            
            if not financial_result["success"]:
                print(f"  ❌ 財務データ抽出エラー: {financial_result['error']}")
                results.append({
                    "企業名": actual_name,
                    "docID": doc_id,
                    "docDescription": doc_description,
                    "提出日": submit_date,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": financial_result["error"]
                })
                continue
            
            # 結果をまとめる
            financial_data = financial_result["data"]
            
            company_data = {
                "企業名": actual_name,
                "docID": doc_id,
                "docDescription": doc_description,
                "提出日": submit_date,
                "売上高": financial_data.get("売上高"),
                "資本金": financial_data.get("資本金"),
                "従業員数": financial_data.get("従業員数"),
            }
            
            # 関連企業情報がある場合は追加
            if "関連企業情報" in financial_data:
                company_data["関連企業情報"] = financial_data["関連企業情報"]
            
            results.append(company_data)
            
            # 抽出成功の指標データ数をカウント
            extracted_count = sum(1 for v in [
                financial_data.get("売上高"), 
                financial_data.get("資本金"), 
                financial_data.get("従業員数")
            ] if v is not None)
            
            print(f"  ✅ 完了: {extracted_count}/3 個の財務指標を抽出しました")
            
            # 財務データの概要表示
            if financial_data.get("売上高"):
                print(f"     💰 売上高: {financial_data.get('売上高'):,.0f}円")
            if financial_data.get("資本金"):
                print(f"     🏦 資本金: {financial_data.get('資本金'):,.0f}円")
            if financial_data.get("従業員数"):
                print(f"     👥 従業員数: {financial_data.get('従業員数'):,.0f}人")
        
        # DataFrameに変換
        df = pd.DataFrame(results)
        return df

def main():
    # APIキーを設定
    api_key = os.getenv("EDINET_API_KEY")
    
    if not api_key:
        print("ERROR: APIキーが設定されていません。")
        print("環境変数 EDINET_API_KEY を設定してください。")
        return
    
    # データ抽出クライアント初期化
    extractor = EDINETCompanyExtractor(api_key)
    
    # 対象企業名リスト
    company_names = [
        "NTTデータ",
        "富士通",
        "野村総合研究所",
        "日本電信電話",
        "TIS"
    ]
    
    print("=== EDINET API v2 - 最新有価証券報告書抽出ツール ===\\n")
    
    # データ抽出実行（過去730日を検索して最新の有価証券報告書を確実に取得）
    results_df = extractor.extract_companies_data(company_names, search_days=730)
    
    if results_df.empty:
        print("\\n❌ データが取得できませんでした。")
        return
    
    print(f"\\n{'='*80}")
    print(f"=== 最終抽出結果 ===")
    print(f"{'='*80}")
    
    # 結果を見やすくフォーマットして表示
    display_df = results_df.copy()
    
    # 数値をフォーマット
    for col in ['売上高', '資本金']:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,.0f}円" if pd.notnull(x) and x != 0 else "データなし"
            )
    
    if '従業員数' in display_df.columns:
        display_df['従業員数'] = display_df['従業員数'].apply(
            lambda x: f"{x:,.0f}人" if pd.notnull(x) and x != 0 else "データなし"
        )
    
    print(display_df.to_string(index=False))
    
    # ファイル保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # CSV保存（生データ）
    csv_filename = f"latest_securities_reports_{timestamp}.csv"
    results_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"\\n📁 生データをCSVファイルに保存: {csv_filename}")
    
    # Excel保存（フォーマット済み）
    excel_filename = f"latest_securities_reports_{timestamp}.xlsx"
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        # 生データシート
        results_df.to_excel(writer, sheet_name='生データ', index=False)
        # フォーマット済みシート
        display_df.to_excel(writer, sheet_name='フォーマット済み', index=False)
    
    print(f"📁 結果をExcelファイルに保存: {excel_filename}")
    print(f"   - '生データ'シート: 元の数値データ")
    print(f"   - 'フォーマット済み'シート: 見やすく整形されたデータ")
    
    # 統計情報の詳細表示
    success_count = len(results_df[results_df["エラー"].isna()]) if "エラー" in results_df.columns else len(results_df)
    
    print(f"\\n📊 抽出統計:")
    print(f"   📈 処理企業数: {len(results_df)} 社")
    print(f"   ✅ 成功企業数: {success_count} 社")
    print(f"   📊 成功率: {success_count/len(results_df):.1%}")
    
    # 成功した企業の詳細
    if success_count > 0:
        successful_companies = results_df[results_df["エラー"].isna()] if "エラー" in results_df.columns else results_df
        print(f"\\n✅ 成功した企業:")
        for _, row in successful_companies.iterrows():
            submit_date = row['提出日'] if '提出日' in row else 'N/A'
            print(f"   • {row['企業名']} (提出日: {submit_date})")
    
    # 失敗した企業の詳細
    if success_count < len(results_df):
        failed_companies = results_df[results_df["エラー"].notna()] if "エラー" in results_df.columns else pd.DataFrame()
        if not failed_companies.empty:
            print(f"\\n❌ 失敗した企業:")
            for _, row in failed_companies.iterrows():
                print(f"   • {row['企業名']}: {row['エラー']}")

if __name__ == "__main__":
    main()
