# Databricks notebook source
# MAGIC %md
# MAGIC # はじめてのDatabricks
# MAGIC
# MAGIC このノートブックではDatabricksの基本的な使い方をご説明します。
# MAGIC
# MAGIC [はじめてのDatabricks](https://qiita.com/taka_yayoi/items/8dc72d083edb879a5e5d)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricksの使い方
# MAGIC
# MAGIC ![how_to_use.png](./img/how_to_use.png "how_to_use.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 画面の説明
# MAGIC
# MAGIC Databricksでデータ分析を行う際に頻繁に使用するのが、今画面に表示している**ノートブック**です。AIアシスタントをはじめ、分析者の生産性を高めるための工夫が随所に施されています。
# MAGIC
# MAGIC - [日本語設定](https://qiita.com/taka_yayoi/items/ff4127e0d632f5e02603)
# MAGIC - [ワークスペース](https://docs.databricks.com/ja/workspace/index.html)
# MAGIC - [ノートブック](https://docs.databricks.com/ja/notebooks/index.html)の作成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 計算資源(コンピュート)
# MAGIC
# MAGIC Databricksにおける計算資源は[コンピュート](https://docs.databricks.com/ja/compute/index.html)と呼ばれます。Databricksが全てを管理する**サーバレス**の計算資源も利用できます。
# MAGIC
# MAGIC - コンピュート
# MAGIC - SQLウェアハウス
# MAGIC
# MAGIC ここでは、画面の右上にある**接続**ボタンをおして、**サーバレスコンピュート**を選択します。
# MAGIC
# MAGIC ![](img/compute1.png)
# MAGIC
# MAGIC **接続済み**となれば計算資源を使用してプログラムを実行することができます。
# MAGIC
# MAGIC ![](img/compute1.png)
# MAGIC
# MAGIC プログラムを実行するにはセルの左上にある**▶︎**ボタンをクリックします。

# COMMAND ----------

# Databricksノートブックの動作確認用メッセージを出力
print("Hello Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの読み込み
# MAGIC
# MAGIC Databricksでは上述のコンピュートを用いて、ファイルやテーブルにアクセスします。SQLやPython、Rなどを活用することができます。Databricksノートブックではこれらの[複数の言語を混在](https://docs.databricks.com/ja/notebooks/notebooks-code.html#mix-languages)させることができます。
# MAGIC
# MAGIC ![how_to_use2.png](./img/how_to_use2.png "how_to_use2.png")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC AIアシスタントを活用してロジックを組み立てることができます。アシスタントを呼び出すにはセルの上にカーソルを合わせた際に表示される![assistant.png](./img/assistant.png "assistant.png")アイコンをクリックします。
# MAGIC
# MAGIC - [ビジュアライゼーション/データプロファイル](https://docs.databricks.com/ja/visualizations/index.html)
# MAGIC - [AIアシスタント](https://docs.databricks.com/ja/notebooks/databricks-assistant-faq.html)
# MAGIC
# MAGIC プロンプト: `samples.tpch.ordersの中身を1000行表示`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **参考資料**
# MAGIC
# MAGIC - [Databricksドキュメント \| Databricks on AWS](https://docs.databricks.com/ja/index.html)
# MAGIC - [はじめてのDatabricks](https://qiita.com/taka_yayoi/items/8dc72d083edb879a5e5d)
# MAGIC - [Databricksチュートリアル](https://qiita.com/taka_yayoi/items/4603091dd325c77d577f)
# MAGIC - [Databricks記事のまとめページ\(その1\)](https://qiita.com/taka_yayoi/items/c6907e2b861cb1070f4d)
# MAGIC - [Databricks記事のまとめページ\(その2\)](https://qiita.com/taka_yayoi/items/68fc3d67880d2dcb32bb)
