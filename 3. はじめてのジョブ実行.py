# Databricks notebook source
# MAGIC %md
# MAGIC # はじめてのジョブ実行
# MAGIC
# MAGIC 先ほどは手動で個々のノートブックを実行してタスクを達成しました。開発フェーズはこれで問題ありませんが、ロジックが固まったら自動で処理が実行されるようにすべきです。そのための機能が[Databricksワークフロー(ジョブ)](https://learn.microsoft.com/ja-jp/azure/databricks/jobs/)です。
# MAGIC
# MAGIC Databricksでジョブを構成するのは非常に簡単です。前のハンズオンで使用したノートブックをそのままジョブ化することができます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ジョブの作成
# MAGIC
# MAGIC これでジョブを作成する準備が整いました。サイドメニューから**ジョブとパイプライン**にアクセスして、**作成 > ジョブ**をクリックします。
# MAGIC
# MAGIC ![](img/job1.png)
# MAGIC
# MAGIC ジョブは複数のタスクから構成することができ、タスクにはノートブックやダッシュボードなどを指定することができます。ここでは、上で作成した2つのノートブックをタスクとして指定します。
# MAGIC
# MAGIC **ノートブック**をクリックしてノートブックタスクを追加します。
# MAGIC
# MAGIC ![](img/job3.png)
# MAGIC
# MAGIC **パス**をクリックして上の`1. データ取り込み`ノートブックを選択します。**最近使用したアイテム**タブに先ほどのノートブックが表示されているはずです。
# MAGIC
# MAGIC ![job4.png](img/job4.png)
# MAGIC
# MAGIC ![job5.png](img/job5.png)
# MAGIC
# MAGIC タスク名は英数字とアンダースコアのみが許可されるので、`data_retrieval`といった名前にして**タスクを作成**をクリックすると、1つ目のタスクが作成されます。
# MAGIC
# MAGIC ![](img/job6.png)
# MAGIC
# MAGIC **タスクを追加 > ノートブック**をクリックして後段のタスクを作成します。
# MAGIC
# MAGIC ![](img/job7.png)
# MAGIC
# MAGIC 今度はパスに`2. データ変換とロード`ノートブックを指定して、タスク名を`data_transform_load`として、**タスクを作成**をクリックします。
# MAGIC
# MAGIC ![](img/job8.png)
# MAGIC
# MAGIC 最後にジョブ名をわかりやすいものに変更して準備は完了です。
# MAGIC
# MAGIC ![](img/job9.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ジョブの実行
# MAGIC
# MAGIC ジョブは即時実行やスケジュール実行などが可能です。即時実行するには右上の**今すぐ実行**をクリックします。
# MAGIC
# MAGIC ![](img/job10.png)
# MAGIC
# MAGIC **ジョブの実行**タブに切り替えると、実行中のジョブの進捗を確認できます。
# MAGIC
# MAGIC ![](img/job11.png)
# MAGIC
# MAGIC 問題がなければ処理は正常終了します。
# MAGIC
# MAGIC ![](img/job12.png)
# MAGIC
# MAGIC データパイプラインによって作成されたテーブルをカタログエクスプローラで確認することができます。
# MAGIC
# MAGIC ![](img/job13.png)
# MAGIC
# MAGIC 非常にシンプルなデータパイプラインではありますが、これで一通りの実装とジョブとしての実行を体験することができました！

# COMMAND ----------

# MAGIC %md
# MAGIC ## ジョブのスケジュール実行
# MAGIC
# MAGIC 通常、ジョブは夜間に実行するなどスケジューリングすることが一般的です。ジョブの**トリガーを追加**をクリックします。
# MAGIC
# MAGIC ![Screenshot 2025-01-08 at 14.14.40.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/1168882/27b63971-ae35-4728-154f-d03b13923170.png)
# MAGIC
# MAGIC トリガータイプで**スケジュール済み**を選択します。
# MAGIC
# MAGIC ![Screenshot 2025-01-08 at 14.15.26.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/1168882/a468ccfb-38e6-5d7d-3757-5672d73e3ae1.png)
# MAGIC
# MAGIC スケジュールのタイプで**Advanced**を選択し、タイムゾーンを**UTC+09:00**を選択して、数分後に実行されるように設定します。
# MAGIC
# MAGIC ![Screenshot 2025-01-08 at 14.17.11.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/1168882/7274530f-c4b0-bcbb-1469-a341ac103a8c.png)
# MAGIC
# MAGIC 設定した時間になるとジョブが起動します。
# MAGIC
# MAGIC ![Screenshot 2025-01-08 at 14.18.28.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/1168882/b6e2be18-418d-70de-0ed7-ba8d060da795.png)
# MAGIC
# MAGIC **スケジューリングされたジョブは定期実行されるので、不要になったジョブは一時停止あるいは削除するようにしてください。**
# MAGIC ![Screenshot 2025-01-08 at 14.19.59.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/1168882/a3f61115-4a7a-6803-6da0-4a477789c405.png)
