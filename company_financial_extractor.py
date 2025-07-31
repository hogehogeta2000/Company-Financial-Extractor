import requests
import os
import pandas as pd
from datetime import datetime, timedelta
import re
import time

class EDINETCodeExtractor:
    def __init__(self, subscription_key):
        """
        EDINETコードリスト検索データ抽出クライアント
        """
        self.subscription_key = subscription_key
        self.base_url = "https://disclosure.edinet-fsa.go.jp/api/v2"
        
        # 指定されたカラムの財務指標マッピング
        self.target_indicators = {
            "売上高": ["netsales", "operatingrevenues", "revenue"],
            "資本金": ["capitalstock", "paidincapital", "capital"],
            "従業員数": ["numberofemployees", "employees"]
        }
    
    def get_latest_securities_report(self, edinet_code):
        """
        指定されたEDINETコードの最新有価証券報告書を取得
        """
        print(f"  🔄 EDINETコード {edinet_code} の最新有価証券報告書を検索中...")
        
        # 過去2年間で検索
        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)
        
        latest_report = None
        latest_date = ""
        
        # 最近の日付から逆順で検索（効率化）
        current_date = end_date
        search_days = 0
        max_search_days = 365  # 最大1年間検索
        
        while current_date >= start_date and search_days < max_search_days:
            date_str = current_date.strftime("%Y-%m-%d")
            
            try:
                url = f"{self.base_url}/documents.json"
                params = {
                    "date": date_str,
                    "type": "2",
                    "Subscription-Key": self.subscription_key
                }
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if "results" in data:
                    for result in data["results"]:
                        # EDINETコードと書類種別でフィルタリング
                        if (result.get("edinetCode") == edinet_code and 
                            result.get("docTypeCode") == "120"):
                            
                            submit_datetime = result.get("submitDateTime", "")
                            if submit_datetime > latest_date:
                                latest_date = submit_datetime
                                latest_report = result
                
                # 報告書が見つかったら検索終了
                if latest_report:
                    break
                
                # API制限対策
                time.sleep(0.5)
                
            except Exception as e:
                print(f"    ⚠️ {date_str} の検索でエラー: {str(e)}")
            
            current_date -= timedelta(days=7)  # 1週間ずつ遡る
            search_days += 7
        
        if latest_report:
            submit_date = latest_report.get("submitDateTime", "")[:10]
            doc_description = latest_report.get("docDescription", "")
            filer_name = latest_report.get("filerName", "")
            print(f"  ✅ 最新報告書発見: {submit_date} 提出")
            print(f"     企業名: {filer_name}")
            print(f"     書類: {doc_description}")
            return latest_report
        else:
            print(f"  ❌ 有価証券報告書が見つかりません")
            return None
    
    def get_xbrl_document(self, doc_id):
        """XBRLドキュメントを取得"""
        url = f"{self.base_url}/documents/{doc_id}"
        
        params = {
            "type": "5",  # CSV形式のXBRLデータ
            "Subscription-Key": self.subscription_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
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
                
                row.append(current_field.strip('"'))
                
                if len(row) >= 6:
                    data_rows.append(row)
            
            if not data_rows:
                return {"success": False, "error": "有効なデータ行が見つかりません"}
            
            headers = data_rows[0] if data_rows else []
            data_dict = {}
            
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
                                    if indicator_name not in data_dict or len(keyword) > len(data_dict.get(f"{indicator_name}_keyword", "")):
                                        cleaned_value = self._clean_numeric_value(value)
                                        if cleaned_value is not None:
                                            data_dict[indicator_name] = cleaned_value
                                            data_dict[f"{indicator_name}_keyword"] = keyword
                                    break
            
            # 内部キーワード情報を削除
            data_dict = {k: v for k, v in data_dict.items() if not k.endswith('_keyword')}
            
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
            cleaned = re.sub(r'[,¥円]', '', str(value_str))
            cleaned = re.sub(r'[^\\d.-]', '', cleaned)
            
            if cleaned and cleaned != '-':
                return float(cleaned)
            return None
        except:
            return None
    
    def extract_companies_data_by_edinet_codes(self, edinet_codes):
        """
        EDINETコードリストから財務データを抽出
        
        Args:
            edinet_codes (list): EDINETコードのリスト
        
        Returns:
            pd.DataFrame: 抽出結果のDataFrame
        """
        print(f"=== EDINET EDINETコードリスト検索版 ===")
        print(f"対象EDINETコード数: {len(edinet_codes)}")
        print(f"検索コード: {', '.join(edinet_codes)}")
        print("=" * 60)
        
        results = []
        
        for i, edinet_code in enumerate(edinet_codes, 1):
            print(f"\\n[{i}/{len(edinet_codes)}] EDINETコード {edinet_code} の財務データ抽出中...")
            
            # 最新の有価証券報告書を取得
            report = self.get_latest_securities_report(edinet_code)
            
            if not report:
                error_msg = "最新の有価証券報告書が見つかりません"
                print(f"  ❌ エラー: {error_msg}")
                results.append({
                    "EDINETコード": edinet_code,
                    "企業名": None,
                    "docID": None,
                    "docDescription": None,
                    "提出日": None,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": error_msg
                })
                continue
            
            doc_id = report.get("docID")
            doc_description = report.get("docDescription", "")
            submit_date = report.get("submitDateTime", "")[:10] if report.get("submitDateTime") else ""
            filer_name = report.get("filerName", "")
            
            print(f"  🆔 docID: {doc_id}")
            
            # XBRLデータを取得
            print(f"  🔄 XBRLデータを取得中...")
            xbrl_result = self.get_xbrl_document(doc_id)
            
            if not xbrl_result["success"]:
                print(f"  ❌ XBRLデータ取得エラー: {xbrl_result['error']}")
                results.append({
                    "EDINETコード": edinet_code,
                    "企業名": filer_name,
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
                    "EDINETコード": edinet_code,
                    "企業名": filer_name,
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
                "EDINETコード": edinet_code,
                "企業名": filer_name,
                "docID": doc_id,
                "docDescription": doc_description,
                "提出日": submit_date,
                "売上高": financial_data.get("売上高"),
                "資本金": financial_data.get("資本金"),
                "従業員数": financial_data.get("従業員数"),
            }
            
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
        print("\\n設定方法:")
        print("export EDINET_API_KEY='your_api_key_here'")
        return
    
    # データ抽出クライアント初期化
    extractor = EDINETCodeExtractor(api_key)
    
    # サンプル: EDINETコードリスト（手作業で取得済み）
    edinet_codes = [
        "E00331",  # サンプルコード1
        "E00334",  # サンプルコード2
        "E00335"   # サンプルコード3
    ]
    
    print("=== EDINET API - EDINETコードリスト検索版（サンプル） ===\\n")
    
    # データ抽出実行
    results_df = extractor.extract_companies_data_by_edinet_codes(edinet_codes)
    
    if results_df.empty:
        print("\\n❌ データが取得できませんでした。")
        return
    
    print(f"\\n{'='*80}")
    print(f"=== 最終抽出結果 ===")
    print(f"{'='*80}")
    
    # 結果を表示
    print(results_df.to_string(index=False))
    
    # CSVファイルに保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"edinet_codes_result_{timestamp}.csv"
    
    results_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"\\n📁 結果をCSVファイルに保存: {csv_filename}")
    
    # 統計情報
    success_count = len(results_df[results_df["エラー"].isna()]) if "エラー" in results_df.columns else len(results_df)
    
    print(f"\\n📊 抽出統計:")
    print(f"   📈 処理EDINETコード数: {len(results_df)} 件")
    print(f"   ✅ 成功数: {success_count} 件")
    print(f"   📊 成功率: {success_count/len(results_df):.1%}")
    print(f"   ⚡ 処理時間: 高速（EDINETコード直接検索）")
    
    print(f"\\n💡 使用方法:")
    print(f"   1. edinet_codes リストに手作業で取得したEDINETコードを追加")
    print(f"   2. スクリプトを実行")
    print(f"   3. CSV形式で結果を取得")

# 追加の便利関数: 複数のEDINETコードを一括処理する場合
def process_edinet_codes_from_list(edinet_code_list):
    """
    EDINETコードのリストを受け取って一括処理する関数
    
    Args:
        edinet_code_list (list): EDINETコードのリスト
    
    Usage:
        codes = ["E00331", "E00334", "E00335", "E12345", "E67890"]
        process_edinet_codes_from_list(codes)
    """
    api_key = os.getenv("EDINET_API_KEY")
    
    if not api_key:
        print("ERROR: APIキーが設定されていません。")
        return None
    
    extractor = EDINETCodeExtractor(api_key)
    
    print(f"=== 一括処理: {len(edinet_code_list)} 個のEDINETコード ===")
    
    results_df = extractor.extract_companies_data_by_edinet_codes(edinet_code_list)
    
    # CSV保存
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"edinet_bulk_result_{timestamp}.csv"
    results_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    
    print(f"\\n📁 結果を保存: {csv_filename}")
    
    return results_df

if __name__ == "__main__":
    main()
