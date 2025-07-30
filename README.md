# Company Financial Extractor

## 📋 プロジェクト概要

**Company Financial Extractor**は、EDINET（Electronic Disclosure for Investors' NETwork）APIを使用して、日本の上場企業の有価証券報告書から財務データを自動抽出するPythonツールです。

企業名のリストを指定するだけで、最新の有価証券報告書から売上高、資本金、従業員数などの主要な財務指標を効率的に取得できます。

## ✨ 主な機能

- 🔍 **企業名での自動検索**: 企業名を指定するだけで該当企業を自動検索
- 📊 **構造化データ抽出**: XBRLデータから財務指標を構造化して取得
- ✅ **事前検証機能**: データ抽出前に企業名のマッチング状況を確認
- 📁 **複数形式出力**: CSV・Excel形式での結果出力
- 🛡️ **エラーハンドリング**: 企業が見つからない場合の適切なエラー処理

## 📈 取得可能なデータ

| 項目 | 説明 |
|------|------|
| **docID** | EDINET書類ID |
| **企業名** | 正式な企業名称 |
| **docDescription** | 有価証券報告書の詳細説明 |
| **売上高** | 企業の年間売上高（円） |
| **資本金** | 企業の資本金額（円） |
| **従業員数** | 連結従業員数（人） |
| **関連企業情報** | グループ企業情報（取得可能な場合） |

## 🚀 セットアップ

### 1. 必要な環境

- Python 3.7以上
- EDINET APIキー（無料で取得可能）

### 2. インストール

```bash
# リポジトリをクローン
git clone https://github.com/your-username/company_financial_extractor.git
cd company_financial_extractor

# 必要なライブラリをインストール
pip install -r requirements.txt
```

### 3. requirements.txt

```
requests>=2.25.0
pandas>=1.3.0
openpyxl>=3.0.0
```

### 4. EDINET APIキーの取得と設定

#### APIキーの取得方法
1. [EDINET API利用登録ページ](https://disclosure.edinet-fsa.go.jp/EKW0EZ0001.html)にアクセス
2. 利用規約に同意して登録
3. メールで送信されるAPIキーを確認

#### APIキーの設定方法

**方法A: 環境変数で設定（推奨）**
```bash
# Linux/Mac
export EDINET_API_KEY="your_api_key_here"

# Windows
set EDINET_API_KEY=your_api_key_here
```

**方法B: .envファイルで設定**
```bash
# .envファイルを作成
echo "EDINET_API_KEY=your_api_key_here" > .env
```

**方法C: スクリプト内で直接指定**
```python
# main()関数内で直接指定
api_key = "your_api_key_here"
```

## 💻 使用方法

### 基本的な使い方

```python
from edinet_extractor import EDINETCompanyNameExtractor

# APIキーを設定
api_key = "your_api_key_here"
extractor = EDINETCompanyNameExtractor(api_key)

# 企業名リストを指定
company_names = [
    "NTTデータ",
    "富士通",
    "野村総合研究所",
    "ソフトバンクグループ"
]

# データ抽出実行
results_df = extractor.extract_companies_data(company_names)

# 結果を表示
print(results_df)
```

### コマンドラインから実行

```bash
python main.py
```

### 実行例

#### 1. 企業名の事前検証
```
=== 企業名の事前検証 ===
✅ NTTデータ → 株式会社エヌ・ティ・ティ・データ (類似度: 85%)
✅ 富士通 → 富士通株式会社 (類似度: 92%)
✅ 野村総合研究所 → 株式会社野村総合研究所 (類似度: 88%)
❌ 存在しない会社: 企業名に該当する企業が見つかりません

成功率: 75% (3/4)
```

#### 2. 財務データ抽出結果
```
企業名                     docID      docDescription              売上高          資本金        従業員数
株式会社エヌ・ティ・ティ・データ  S100R4VZ   有価証券報告書－第36期    1540000000000   139537000000   150000
富士通株式会社              S100R5AB   有価証券報告書－第124期   3710000000000   324625000000   124000
株式会社野村総合研究所       S100R6CD   有価証券報告書－第57期    667900000000    18600000000    15000
```

## 📊 出力ファイル

### CSV形式
- ファイル名: `company_financial_data_YYYYMMDD_HHMMSS.csv`
- エンコード: UTF-8 with BOM（Excel対応）

### Excel形式
- ファイル名: `company_financial_data_YYYYMMDD_HHMMSS.xlsx`
- **財務データシート**: 抽出結果
- **企業名検証シート**: マッチング状況

## ⚙️ カスタマイズ

### 1. 対象企業の変更

```python
# main.py内の企業名リストを編集
company_names = [
    "トヨタ自動車",
    "ソニーグループ",
    "三菱UFJフィナンシャル・グループ",
    "ソフトバンクグループ"
]
```

### 2. 類似度閾値の調整

```python
# より厳密なマッチングを求める場合
if best_score < 0.8:  # デフォルト: 0.5
```

### 3. 追加の財務指標

```python
# target_indicatorsに新しい指標を追加
self.target_indicators = {
    "売上高": ["netsales", "operatingrevenues"],
    "資本金": ["capitalstock", "paidincapital"],
    "従業員数": ["numberofemployees"],
    # 新しい指標を追加
    "総資産": ["totalassets"],
    "純利益": ["netincome"]
}
```

## 🔧 トラブルシューティング

### よくある問題と解決方法

#### 1. APIキーエラー
```
ERROR: APIキーが設定されていません
```
**解決方法**: 環境変数`EDINET_API_KEY`が正しく設定されているか確認

#### 2. 企業名が見つからない
```
❌ 企業名 'XYZ会社' に該当する企業が見つかりません
```
**解決方法**: 
- 正式な企業名で検索（例: "トヨタ" → "トヨタ自動車"）
- 株式会社の表記を統一
- EDINETに登録されている企業か確認

#### 3. 財務データが取得できない
```
❌ 財務データ抽出エラー
```
**解決方法**:
- 最新の有価証券報告書が提出されているか確認
- XBRLデータの形式が対応しているか確認

#### 4. ライブラリインポートエラー
```
ModuleNotFoundError: No module named 'requests'
```
**解決方法**: 
```bash
pip install -r requirements.txt
```

## 📝 使用上の注意

### 1. EDINET API利用規約の遵守
- APIの利用制限（1日1000回まで等）を確認
- 商用利用の場合は利用規約を確認

### 2. データの精度について
- XBRLデータの構造は企業によって異なる場合があります
- 抽出されたデータは必ず元の有価証券報告書で確認してください

### 3. 個人情報・機密情報の取り扱い
- 取得したデータの取り扱いは各自の責任で行ってください
- 企業の機密情報に該当する可能性があるデータの公開は避けてください

## 🤝 コントリビューション

プルリクエストやイシューの報告を歓迎します！

### 開発環境のセットアップ
```bash
git clone https://github.com/your-username/company_financial_extractor.git
cd company_financial_extractor
pip install -r requirements.txt
pip install -r requirements-dev.txt  # 開発用ライブラリ
```

### テストの実行
```bash
python -m pytest tests/
```

## 📜 ライセンス

MIT License - 詳細は[LICENSE](LICENSE)ファイルを参照してください。

## 🙏 謝辞

- [EDINET](https://disclosure.edinet-fsa.go.jp/) - 金融庁が提供する有価証券報告書等の開示システム
- すべてのコントリビューターの皆様

## 📞 サポート

- バグ報告: [Issues](https://github.com/your-username/company_financial_extractor/issues)
- 機能要望: [Issues](https://github.com/your-username/company_financial_extractor/issues)
- 質問: [Discussions](https://github.com/your-username/company_financial_extractor/discussions)

---

**⭐ このプロジェクトが役に立った場合は、スターをつけていただけると嬉しいです！**
