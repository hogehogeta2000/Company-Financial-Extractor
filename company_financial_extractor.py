import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
import re
from difflib import SequenceMatcher

class EDINETCompanyNameExtractor:
    def __init__(self, subscription_key):
        """
        EDINET企業名検索データ抽出クライアントの初期化
        """
        self.subscription_key = subscription_key
        self.base_url = "https://api.edinet-fsa.go.jp/api/v2"
        self.headers = {
            "Ocp-Apim-Subscription-Key": subscription_key,
            "Content-Type": "application/json"
        }
        
        # 指定されたカラムの財務指標マッピング
        self.target_indicators = {
            "売上高": ["netsales", "operatingrevenues", "revenue", "ordingyrevenues"],
            "資本金": ["capitalstock", "paidincapital", "capital"],
            "従業員数": ["numberofemployees", "employees"]
        }
    
    def search_company_by_name(self, company_name):
        """
        企業名で企業を検索し、最も適合する企業を返す
        
        Args:
            company_name (str): 検索する企業名
            
        Returns:
            dict: 検索結果
        """
        url = f"{self.base_url}/documents.json"
        
        # 過去2年間を検索範囲とする
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        params = {
            "date": end_date,
            "type": "1",  # 企業名での検索
            "code": company_name,  # 企業名を指定
            "Subscription-Key": self.subscription_key
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            
            if "results" not in data or not data["results"]:
                return {
                    "success": False,
                    "error": f"企業名 '{company_name}' に該当する企業が見つかりません"
                }
            
            # 企業名の類似度でソートして最適な企業を選択
            best_match = None
            best_score = 0
            
            for result in data["results"]:
                filer_name = result.get("filerName", "")
                # 類似度を計算
                score = SequenceMatcher(None, company_name, filer_name).ratio()
                
                if score > best_score:
                    best_score = score
                    best_match = result
            
            # 類似度が50%未満の場合はマッチ失敗とする
            if best_score < 0.5:
                return {
                    "success": False,
                    "error": f"企業名 '{company_name}' に十分に類似する企業が見つかりません（最高類似度: {best_score:.2%}）"
                }
            
            return {
                "success": True,
                "company_info": best_match,
                "similarity_score": best_score
            }
            
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"企業検索API呼び出しエラー: {str(e)}"
            }
    
    def search_latest_securities_report(self, edin_code):
        """
        EDINコードを使って最新の有価証券報告書を検索
        
        Args:
            edin_code (str): EDINコード
            
        Returns:
            dict: 検索結果
        """
        url = f"{self.base_url}/documents.json"
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        
        params = {
            "date": end_date,
            "type": "2",  # EDINコードでの検索
            "code": edin_code,
            "Subscription-Key": self.subscription_key
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            
            # 有価証券報告書（120）でフィルタリング
            securities_reports = []
            if "results" in data:
                for result in data["results"]:
                    if "120" in result.get("ordinanceCode", ""):
                        securities_reports.append(result)
            
            if securities_reports:
                # 提出日でソートして最新のものを返す
                securities_reports.sort(key=lambda x: x.get("submitDateTime", ""), reverse=True)
                return {
                    "success": True,
                    "document": securities_reports[0]
                }
            else:
                return {
                    "success": False,
                    "error": "有価証券報告書が見つかりません"
                }
                
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": f"有価証券報告書検索エラー: {str(e)}"
            }
    
    def get_xbrl_document(self, doc_id):
        """XBRLドキュメントを取得"""
        url = f"{self.base_url}/documents/{doc_id}"
        
        params = {
            "type": "5",  # CSV形式のXBRLデータ
            "Subscription-Key": self.subscription_key
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
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
            lines = csv_text.strip().split('\n')
            
            if len(lines) < 2:
                return {"success": False, "error": "CSVデータが不正です"}
            
            # CSVをパース
            data_rows = []
            for line in lines:
                row = [item.strip('"') for item in line.split('","')]
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
                            if isinstance(value, str) and len(value) > 3:  # 意味のある文字列のみ
                                subsidiary_info.append(value)
            
            # 内部キーワード情報を削除
            data_dict = {k: v for k, v in data_dict.items() if not k.endswith('_keyword')}
            
            # グループ企業情報をまとめる
            if subsidiary_info:
                data_dict["関連企業情報"] = "; ".join(set(subsidiary_info[:5]))  # 重複除去して最大5件
            
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
            cleaned = re.sub(r'[^\d.-]', '', cleaned)
            
            if cleaned and cleaned != '-':
                return float(cleaned)
            return None
        except:
            return None
    
    def extract_companies_data(self, company_names):
        """
        複数企業名のデータを抽出
        
        Args:
            company_names (list): 企業名のリスト
            
        Returns:
            pd.DataFrame: 抽出結果のDataFrame
        """
        results = []
        
        print(f"対象企業数: {len(company_names)}")
        print("=" * 60)
        
        for i, company_name in enumerate(company_names, 1):
            print(f"[{i}/{len(company_names)}] 処理中: {company_name}")
            
            # 1. 企業名で企業を検索
            search_result = self.search_company_by_name(company_name)
            
            if not search_result["success"]:
                print(f"  ❌ 企業検索エラー: {search_result['error']}")
                results.append({
                    "企業名": company_name,
                    "docID": None,
                    "docDescription": None,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": search_result["error"]
                })
                continue
            
            company_info = search_result["company_info"]
            edin_code = company_info.get("edinetCode")
            actual_company_name = company_info.get("filerName", company_name)
            similarity = search_result["similarity_score"]
            
            print(f"  🔍 マッチした企業: {actual_company_name} (類似度: {similarity:.2%})")
            
            # 2. 最新の有価証券報告書を検索
            report_result = self.search_latest_securities_report(edin_code)
            
            if not report_result["success"]:
                print(f"  ❌ 有価証券報告書検索エラー: {report_result['error']}")
                results.append({
                    "企業名": actual_company_name,
                    "docID": None,
                    "docDescription": None,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": report_result["error"]
                })
                continue
            
            doc = report_result["document"]
            doc_id = doc.get("docID")
            doc_description = doc.get("docDescription", "")
            
            print(f"  📄 報告書: {doc_description}")
            
            # 3. XBRLデータを取得
            xbrl_result = self.get_xbrl_document(doc_id)
            
            if not xbrl_result["success"]:
                print(f"  ❌ XBRLデータ取得エラー: {xbrl_result['error']}")
                results.append({
                    "企業名": actual_company_name,
                    "docID": doc_id,
                    "docDescription": doc_description,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": xbrl_result["error"]
                })
                continue
            
            # 4. 財務データを抽出
            financial_result = self.extract_financial_data_from_csv(xbrl_result["content"])
            
            if not financial_result["success"]:
                print(f"  ❌ 財務データ抽出エラー: {financial_result['error']}")
                results.append({
                    "企業名": actual_company_name,
                    "docID": doc_id,
                    "docDescription": doc_description,
                    "売上高": None,
                    "資本金": None,
                    "従業員数": None,
                    "エラー": financial_result["error"]
                })
                continue
            
            # 5. 結果をまとめる
            financial_data = financial_result["data"]
            
            company_data = {
                "企業名": actual_company_name,
                "docID": doc_id,
                "docDescription": doc_description,
                "売上高": financial_data.get("売上高"),
                "資本金": financial_data.get("資本金"),
                "従業員数": financial_data.get("従業員数"),
            }
            
            # 関連企業情報がある場合は追加
            if "関連企業情報" in financial_data:
                company_data["関連企業情報"] = financial_data["関連企業情報"]
            
            results.append(company_data)
            
            extracted_count = sum(1 for v in [financial_data.get("売上高"), financial_data.get("資本金"), financial_data.get("従業員数")] if v is not None)
            print(f"  ✅ 完了: {extracted_count}/3 個の指標を抽出")
        
        # DataFrameに変換
        df = pd.DataFrame(results)
        return df
    
    def validate_company_names(self, company_names):
        """
        企業名リストの事前検証
        
        Args:
            company_names (list): 企業名のリスト
            
        Returns:
            dict: 検証結果
        """
        print("=== 企業名の事前検証 ===")
        
        validation_results = []
        valid_companies = []
        invalid_companies = []
        
        for company_name in company_names:
            search_result = self.search_company_by_name(company_name)
            
            if search_result["success"]:
                actual_name = search_result["company_info"].get("filerName", "")
                similarity = search_result["similarity_score"]
                
                validation_results.append({
                    "入力企業名": company_name,
                    "マッチした企業名": actual_name,
                    "類似度": f"{similarity:.2%}",
                    "status": "✅ マッチ"
                })
                valid_companies.append(company_name)
                print(f"✅ {company_name} → {actual_name} (類似度: {similarity:.2%})")
            else:
                validation_results.append({
                    "入力企業名": company_name,
                    "マッチした企業名": "",
                    "類似度": "",
                    "status": f"❌ {search_result['error']}"
                })
                invalid_companies.append(company_name)
                print(f"❌ {company_name}: {search_result['error']}")
        
        return {
            "validation_df": pd.DataFrame(validation_results),
            "valid_companies": valid_companies,
            "invalid_companies": invalid_companies,
            "success_rate": len(valid_companies) / len(company_names) if company_names else 0
        }

def main():
    # APIキーを設定
    api_key = os.getenv("EDINET_API_KEY")
    
    if not api_key:
        print("ERROR: APIキーが設定されていません。")
        print("環境変数 EDINET_API_KEY を設定してください。")
        return
    
    # データ抽出クライアント初期化
    extractor = EDINETCompanyNameExtractor(api_key)
    
    # 対象企業名リスト（例）
    company_names = [
        "NTTデータ",
        "富士通",
        "野村総合研究所",
        "日本電信電話",
        "TIS"
    ]
    
    print("=== EDINET 企業名検索・財務データ抽出 ===\n")
    
    # 1. 企業名の事前検証
    validation_result = extractor.validate_company_names(company_names)
    
    print(f"\n企業名検証結果:")
    print(validation_result["validation_df"].to_string(index=False))
    print(f"\n成功率: {validation_result['success_rate']:.1%} ({len(validation_result['valid_companies'])}/{len(company_names)})")
    
    if validation_result["invalid_companies"]:
        print(f"\n⚠️  以下の企業名は処理をスキップされます:")
        for invalid_name in validation_result["invalid_companies"]:
            print(f"   - {invalid_name}")
    
    # 2. 有効な企業のデータ抽出
    if validation_result["valid_companies"]:
        print(f"\n=== 財務データ抽出開始 ===")
        
        results_df = extractor.extract_companies_data(validation_result["valid_companies"])
        
        print(f"\n=== 抽出結果 ===")
        print(results_df.to_string(index=False))
        
        # 3. ファイル保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # CSV保存
        csv_filename = f"company_financial_data_{timestamp}.csv"
        results_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"\n📁 結果をCSVファイルに保存: {csv_filename}")
        
        # Excel保存
        excel_filename = f"company_financial_data_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            results_df.to_excel(writer, sheet_name='財務データ', index=False)
            validation_result["validation_df"].to_excel(writer, sheet_name='企業名検証', index=False)
        
        print(f"📁 結果をExcelファイルに保存: {excel_filename}")
        
        # 4. 統計情報
        success_count = len(results_df[results_df["エラー"].isna()]) if "エラー" in results_df.columns else len(results_df)
        print(f"\n📊 統計情報:")
        print(f"   処理企業数: {len(results_df)}")
        print(f"   成功企業数: {success_count}")
        print(f"   成功率: {success_count/len(results_df):.1%}")
    
    else:
        print("\n❌ 有効な企業名が見つからないため、データ抽出を実行できません。")

if __name__ == "__main__":
    main()
