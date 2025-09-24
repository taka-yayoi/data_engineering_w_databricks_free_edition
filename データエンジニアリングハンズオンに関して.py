# Databricks notebook source
# MAGIC %md
# MAGIC # データエンジニアリングのハンズオン
# MAGIC
# MAGIC 1. ETL処理
# MAGIC     - 簡単なデータを対象に、データの抽出(Extract)、データの変換(Transform)、ロード(Load)を体験いただきます。ここでは、SparkのPython APIである[PySpark](https://learn.microsoft.com/ja-jp/azure/databricks/pyspark/basics)を使用します。
# MAGIC     - 使用するノートブック：[1. データ取り込み]($1. データ取り込み)、[2. データ変換とロード]($2. データ変換とロード)
# MAGIC 1. はじめてのジョブ実行
# MAGIC     - 上で作成したノートブックが自動で実行されるように[ジョブ](https://learn.microsoft.com/ja-jp/azure/databricks/jobs/)を組んで実行します。
# MAGIC     - 使用するノートブック：[1. データ取り込み]($1. データ取り込み)、[2. データ変換とロード]($2. データ変換とロード)、[3. はじめてのジョブ実行]($3. はじめてのジョブ実行)

# COMMAND ----------

# MAGIC %md
# MAGIC ## データエンジニアリングのマインドセット
# MAGIC
# MAGIC 以下は個人的な経験も踏まえてのデータエンジニアリングに必要なマインドセットとなります。[こちらの記事](https://qiita.com/taka_yayoi/items/cba8021d5f368b3c77bb)も参考になります。
# MAGIC
# MAGIC - ビジネスアナリストやデータサイエンティストから必要とされるデータの種類、量、鮮度、品質を理解する。
# MAGIC - 利用可能なデータソースを理解する。不足がある場合に入手方法を検討する。
# MAGIC - 格納されるデータの実態はファイルであることを理解する。
# MAGIC - データ読み書きのインパクトを理解する。
# MAGIC     - プロダクションシステムに不必要な負荷を与えない(閑散期にデータアクセスを行うなど)。
# MAGIC     - データ書き込み先への権限があることを確認する。
# MAGIC - ソースデータから分析データに至る変換ロジックをスマートに実装する。
# MAGIC     - [メダリオンアーキテクチャ](https://learn.microsoft.com/ja-jp/azure/databricks/lakehouse/medallion)やETL([Extract](https://qiita.com/taka_yayoi/items/08aeeac8f63a2f8158eb)、[Transformation](https://qiita.com/taka_yayoi/items/51bd9a1a895109ec1625)、Load)を理解する。
# MAGIC     - SQLやPython([PySpark](https://learn.microsoft.com/ja-jp/azure/databricks/pyspark/))を習熟し、必要な場合にはSparkなどを用いた並列処理を行う。複雑な処理、パフォーマンスチューニングの柔軟性の観点ではPySparkをマスターすることをお勧めします。
# MAGIC     - 洗い替えでなく[インクリメンタルな処理](https://learn.microsoft.com/ja-jp/azure/databricks/structured-streaming/incremental)や[チェンジデータキャプチャ](https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/cdc)を実装する。
# MAGIC     - [ワークフロー](https://learn.microsoft.com/ja-jp/azure/databricks/jobs/)などを活用して可能な限り自動化する。
# MAGIC - データエンジニアリングで使用するリソース(ジョブ、ノートブック、テーブル、ファイルなど)の所在を適切に管理する。
# MAGIC     - [ワークスペース](https://learn.microsoft.com/ja-jp/azure/databricks/workspace/)のコンセプトを理解する。
# MAGIC     - [ガバナンス](https://learn.microsoft.com/ja-jp/azure/databricks/data-governance/)の重要性を理解する。
