import requests
import json
from typing import Dict, Optional, Any

class GBizInfoAPI:
    """gBizInfo REST API クライアント"""
    
    def __init__(self, api_key: str):
        """
        初期化
        
        Args:
            api_key (str): gBizInfo APIキー
        """
        self.api_key = api_key
        self.base_url = "https://info.gbiz.go.jp/hojin"
        self.headers = {
            "X-hojinInfo-api-token": api_key,
            "Accept": "application/json"
        }
    
    def get_basic_info(self, corporate_number: str) -> Optional[Dict[str, Any]]:
        """
        基本企業情報を取得
        
        Args:
            corporate_number (str): 法人番号（13桁）
            
        Returns:
            Dict[str, Any]: 企業基本情報
        """
        url = f"{self.base_url}/v1/hojin/{corporate_number}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"基本情報取得エラー: {e}")
            return None
    
    def get_finance_info(self, corporate_number: str) -> Optional[Dict[str, Any]]:
        """
        財務情報を取得
        
        Args:
            corporate_number (str): 法人番号（13桁）
            
        Returns:
            Dict[str, Any]: 財務情報
        """
        url = f"{self.base_url}/v1/hojin/{corporate_number}/finance"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"財務情報取得エラー: {e}")
            return None
    
    def extract_company_data(self, corporate_number: str) -> Dict[str, Any]:
        """
        企業の主要情報を抽出
        
        Args:
            corporate_number (str): 法人番号（13桁）
            
        Returns:
            Dict[str, Any]: 抽出した企業情報
        """
        result = {
            "corporate_number": corporate_number,
            "company_name": None,
            "capital": None,
            "employees": None,
            "revenue": None,
            "website_url": None,
            "status": "success"
        }
        
        # 基本情報を取得
        basic_info = self.get_basic_info(corporate_number)
        if not basic_info:
            result["status"] = "error: 基本情報取得失敗"
            return result
        
        # 基本情報から抽出
        try:
            hojin_infos = basic_info.get("hojin-infos", [])
            if hojin_infos:
                hojin_info = hojin_infos[0]
                
                # 会社名
                result["company_name"] = hojin_info.get("name")
                
                # 資本金
                if "capital" in hojin_info:
                    result["capital"] = hojin_info["capital"]
                
                # 従業員数
                if "employee-number" in hojin_info:
                    result["employees"] = hojin_info["employee-number"]
                
                # WebサイトURL
                if "homepage-url" in hojin_info:
                    result["website_url"] = hojin_info["homepage-url"]
        except Exception as e:
            print(f"基本情報解析エラー: {e}")
        
        # 財務情報を取得
        finance_info = self.get_finance_info(corporate_number)
        if finance_info:
            try:
                finance_infos = finance_info.get("finance-infos", [])
                if finance_infos:
                    # 最新の財務情報を取得
                    latest_finance = finance_infos[0]
                    
                    # 売上高
                    if "revenue" in latest_finance:
                        result["revenue"] = latest_finance["revenue"]
                    elif "sales" in latest_finance:
                        result["revenue"] = latest_finance["sales"]
                        
            except Exception as e:
                print(f"財務情報解析エラー: {e}")
        
        return result
    
    def format_output(self, data: Dict[str, Any]) -> str:
        """
        取得データを整形して出力
        
        Args:
            data (Dict[str, Any]): 企業データ
            
        Returns:
            str: 整形された出力文字列
        """
        if data["status"] != "success":
            return f"エラー: {data['status']}"
        
        output = f"""
企業情報調査結果
================
法人番号: {data['corporate_number']}
会社名: {data['company_name'] or 'データなし'}
資本金: {data['capital'] or 'データなし'}
従業員数: {data['employees'] or 'データなし'}
売上高: {data['revenue'] or 'データなし'}
WebサイトURL: {data['website_url'] or 'データなし'}
        """
        return output.strip()

# 使用例
def main():
    # APIキーを設定（実際のキーに置き換えてください）
    API_KEY = "your_api_key_here"
    
    # APIクライアントを初期化
    api_client = GBizInfoAPI(API_KEY)
    
    # 調査対象の法人番号（例：トヨタ自動車の法人番号）
    corporate_numbers = [
        "5030001007261",  # 例：トヨタ自動車
        "9010001021751",  # 例：ソフトバンクグループ
        # 他の法人番号を追加
    ]
    
    # 各企業の情報を調査
    for corp_num in corporate_numbers:
        print(f"\n=== 法人番号: {corp_num} の調査開始 ===")
        
        # 企業データを取得・抽出
        company_data = api_client.extract_company_data(corp_num)
        
        # 結果を表示
        print(api_client.format_output(company_data))
        
        # JSONファイルに保存（オプション）
        filename = f"company_data_{corp_num}.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(company_data, f, ensure_ascii=False, indent=2)
            print(f"データを {filename} に保存しました")
        except Exception as e:
            print(f"ファイル保存エラー: {e}")

if __name__ == "__main__":
    main()
