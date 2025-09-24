# Databricks notebook source
# MAGIC %md
# MAGIC # データ取り込み
# MAGIC
# MAGIC このノートブックはETL処理における **E(Extract:抽出)** に該当します。Pythonを用いてインターネットからCSVファイルを取得します。
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
# MAGIC ## Unity Catalog
# MAGIC
# MAGIC Databricksの資産(テーブルやファイルなど)はすべて[Unity Catalog](https://learn.microsoft.com/ja-jp/azure/databricks/data-governance/unity-catalog/)によって管理されます。データエンジニアリングを行う際には、対象のデータがUnity Catalogのどこに存在しているのかを理解することはとても重要です。
# MAGIC
# MAGIC Unity Catalogを用いることで、以下の機能でDatabricks上の資産に対するガバナンスを強化することができます。
# MAGIC
# MAGIC - **データアクセス制御**
# MAGIC   - 一度定義するだけで、全てのワークスペース、全ての言語、全てのユースケースに対してセキュリティを適用
# MAGIC - **監査機能**
# MAGIC   - ガバナンスユースケースに対応するために、全てのクエリーに対するきめ細かい監査を実現
# MAGIC - **データリネージ**
# MAGIC   - テーブル、カラムに対するデータリネージの自動収集
# MAGIC - **データ探索**
# MAGIC   - お使いのレイクハウスにおいてデータ利用者が信頼できるデータを検索するためのインタフェースを提供
# MAGIC - **属性ベースのアクセス制御**
# MAGIC   - 行列レベルのネイティブなセキュリティ、タグによるポリシー適用
# MAGIC
# MAGIC ### 3レベルの名前空間
# MAGIC
# MAGIC Unity Catalogはデータを整理するために3レベルの名前空間、カタログ、スキーマ(データベースとも呼ばれます)、テーブルとビューを提供します。テーブルを参照するには以下の文法を使用します。
# MAGIC
# MAGIC >`<catalog>.<schema>.<table>`
# MAGIC
# MAGIC ![3_level_namespace.png](./img/3_level_namespace.png "3_level_namespace.png")
# MAGIC
# MAGIC ファイルは[ボリューム](https://learn.microsoft.com/ja-jp/azure/databricks/volumes/)という場所に格納されます。ボリュームはテーブルと同様`カタログ.スキーマ(データベース).ボリューム`という三階層で管理されます。また、ボリュームに格納されているファイルには[パス](https://learn.microsoft.com/ja-jp/azure/databricks/data-governance/unity-catalog/paths)を指定してアクセスすることができます。データエンジニアリングにおいては、ファイルの取り扱いは不可欠ですので、パスの考え方に慣れるようにしましょう。
# MAGIC
# MAGIC `<catalog_name>.<schema_name>.<volume_name>`で管理されているボリュームには、PythonやSQLから`/Volumes/<catalog_name>/<schema_name>/<volume_name>/<path_to_file>`というパスでアクセスすることができます。
# MAGIC
# MAGIC まず、今回のハンズオンで使用するカタログ、スキーマ、ボリュームを設定します。

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## カタログの指定
# MAGIC
# MAGIC 使用するカタログやスキーマ(データベース)にアクセスするには明示的に指定する必要があります。`USE CATALOG`文を用いて、上で表示される作成済みのカタログを使用します。

# COMMAND ----------

# 使用するカタログを指定
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## スキーマ(データベース)の作成
# MAGIC
# MAGIC 皆様のユーザー名(メールアドレス)をベースとしたスキーマを作成します。

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
# MAGIC ## ボリュームの作成
# MAGIC
# MAGIC カタログとスキーマ(データベース)の準備ができたので、ボリュームを作成します。

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のコマンドを実行して表示されるリンクをクリックしてボリュームを確認します。リンク先の画面は[カタログエクスプローラ](https://learn.microsoft.com/ja-jp/azure/databricks/catalog-explorer/)と呼ばれるものであり、Unity Catalogで管理されている資産をGUIから確認、操作することができます。

# COMMAND ----------

displayHTML(f"<a href='/explore/data/volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}' target='_blank'>作成したボリュームを表示</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの取り込み
# MAGIC
# MAGIC [requests](https://pypi.org/project/requests/)を用いてインターネットからCSVファイルを取得し、[dbutils.fs.put](https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils#dbutils-fs-cp)を用いてファイルをボリュームにコピーします。ここでは[ニューヨークの公開データ](https://health.data.ny.gov/)を使用します。

# COMMAND ----------

# データを取得して保存する
import requests

# CSVファイルを取得
response = requests.get('https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv')
csvfile = response.content.decode('utf-8')

# CSVファイルをボリュームに保存
dbutils.fs.put(f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/babynames.csv", csvfile, True)

# COMMAND ----------

# MAGIC %md
# MAGIC もう一度カタログエクスプローラにアクセスして、ボリュームにファイルが保存されていることを確認しましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの確認
# MAGIC
# MAGIC 以下のコマンドは、SparkのPython APIである[PySpark](https://learn.microsoft.com/ja-jp/azure/databricks/pyspark/basics)を用いてCSVを読み込み内容を表示します。これが今回のハンズオンで使用する生データとなります。

# COMMAND ----------

# CSVファイルを読み込む
babynames = (
    spark.read.format("csv") # CSVフォーマット
    .option("header", "true") # ヘッダーあり
    .option("inferSchema", "true") # スキーマの推定
    .load(f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/babynames.csv") # ファイルパスを指定
)
display(babynames)
