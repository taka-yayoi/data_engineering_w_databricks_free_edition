# Databricks notebook source
# MAGIC %md
# MAGIC # データの変換とロード
# MAGIC
# MAGIC このノートブックはETL処理における **T(Transform:抽出)** と **L(Load:ロード)** に該当します。ボリュームのファイルのデータの変換処理を行い、Unity Catalogのテーブルとしてロードします。
# MAGIC
# MAGIC こちらは、[Azure Databricks ジョブを使用して最初のワークフローを作成する](https://learn.microsoft.com/ja-jp/azure/databricks/jobs/jobs-quickstart)のサンプルノートブックをベースとしています。
# MAGIC
# MAGIC まず、このノートブックを[ノートブック用のサーバレスコンピューティング](https://learn.microsoft.com/ja-jp/azure/databricks/compute/serverless/notebooks)で実行し、その後で[ジョブ用のサーバレスコンピューティング](https://learn.microsoft.com/ja-jp/azure/databricks/jobs/run-serverless-jobs)を用いてジョブとして実行します。
# MAGIC
# MAGIC ノートブック右上の**接続**から**サーバレス**を選択してください。
# MAGIC
# MAGIC ![](https://sajpstorage.blob.core.windows.net/yayoi/202412_handson/serverless_notebook.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定
# MAGIC
# MAGIC まず、今回のハンズオンで使用するカタログ、スキーマ、ボリュームを設定します。

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## カタログの指定
# MAGIC
# MAGIC 上で表示される作成済みのカタログを使用します。

# COMMAND ----------

# 使用するカタログを指定
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 皆様のユーザー名からスキーマ名を生成し、スキーマを作成
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS database_name;
# MAGIC DECLARE database_name = concat("schema_", regexp_replace(session_user(), '[\.@-]', '_'));
# MAGIC CREATE DATABASE IF NOT EXISTS IDENTIFIER(database_name);
# MAGIC SELECT database_name;

# COMMAND ----------

# スキーマ名(データベース)
SCHEMA_NAME = _sqldf.first()["database_name"]
print(f"ハンズオンで使用するスキーマは {SCHEMA_NAME} です。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの変換

# COMMAND ----------

# CSVファイルを読み込む
babynames = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/babynames.csv")
)

# 年を整数型に変換し、2014年のデータをフィルタリング(Transform)
babynames_transformed = (
    babynames.withColumnRenamed("First Name", "First_Name")  # 列名を変更
    .withColumn("Year", babynames["Year"].cast("int"))  # 年を整数型に変換
    .filter(babynames["Year"] == 2014)  # 2014年のデータをフィルタリング
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ロード
# MAGIC
# MAGIC 変換処理を行ったデータを他のユーザーが利用できるように、テーブルにロード(保存)します。

# COMMAND ----------

# テーブルに保存(Load)
babynames_transformed.write.mode("overwrite").saveAsTable(
    f"{CATALOG_NAME}.{SCHEMA_NAME}.babynames_table"
)

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のコマンドを実行して表示されるリンクをクリックしてテーブルを確認します。

# COMMAND ----------

displayHTML(f"<a href='/explore/data/{CATALOG_NAME}/{SCHEMA_NAME}/babynames_table' target='_blank'>作成したテーブルを表示</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC 次のステップでこのテーブルを再作成するので、カタログエクスプローラで削除しておきます。
# MAGIC
# MAGIC ![](img/drop_table.png)
