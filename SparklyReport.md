# Sparkly report

## 项目描述：

Sparkify这是一个虚拟的音乐服务数据集，拥有过千万用户，用户可以随时升级、降级、取消他们的套餐。用户的动态、意向可以直接影响到服务的盈利；而每次用户的操作都会被记录(即具体动作例如收藏、点赞、差评、播放歌曲、播放时长等)，这些数据对于服务商而言有着重要价值，可从该数据中发现某些用户的某些操作的共通点，来判断该用户接下来会进行什么样的操作， 项目的目标是寻找潜在客户，而潜在客户也分为潜在意向客户和流失客户。利用Spark操纵大的真实数据集来抽取相关性特征来预测客户流失情况,使用 Spark MLib 搭建机器学习模型处理大数据集，利用机器学习找到那些流失的客户。

## 项目目的：

项目的目的是通过对于Sparkify音乐服务数据集各个特征的分析，建立模型来预测用户流失的情况。

通过如下的步骤来进行处理：

- 对于提供的数据集中的数据进行分析，对数据进行清理，修复或者删除异常值，删除不能为任务提供有用价值的数据。对数据集中各个特征值的关系进行可视化的分析；
- 研究各种机器学习的模型，选择合理的模型进行训练，对于用户流失的情况进行预测和评估；

## 项目流程：

- 将大数据集加载到 Spark 上，并使用 Spark SQL 和 Spark 数据框查看数据集特征；
- 利用spark和pandas对数据集进行清洗；
- 探索性分析数据，查看各个特征之间的关系；
- 在 Spark ML 中使用机器学习 API 来搭建和调整模型；
- 对搭建的模型进行调参；



## 指标：

### 准确率

准确率能够判断总的正确率，但是在样本不均衡的情况下，并不能作为很好的指标来衡量结果，该指标作为模型评价的备选项；

### F1 分数

使用调和平均结合召回率和精度的指标，同时兼顾了分类模型的精确率和召回率。

该项目中由于涉及到分类，因此采用F1分数作为模型的评价指标。

## 原始数据分析：

加载数据：

```
sparkify_data = 'mini_sparkify_event_data.json'
df = spark.read.json(sparkify_data)
```

通过printschema输出全部的特征

```
root
 |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: long (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true
```



通过统计：

- artist，firstName, gender, lastName, length,  location, registration, song,  userAgen存在nan项；

- userId存在为空的项；



数据集中前几行的数据项：

![截屏2020-03-24上午9.52.01](./img/columns.png)



## 数据清理：

###  清理数据

- 清理userId特征为NaN和为空的数据项，由于用户信息的缺失对于这些数据的分析没有意义；

- 对于artist、length、song字段为空的数据保留着部分的数据，可能存在用户并没有收听任何的歌曲；

- 对于name项（firstName和lastName）为空的数据继续保留，使用userId作为唯一的用户的区分；

  
  
  清理userId为空的数据项
  
  ````
  # 清除userId数据项为空的数据
  df = df.filter(df.userId!="")
  ````

​      清除userId和sessionId为空的数据项

       ```
df = df.dropna(how = "any", subset = ["userId", "sessionId"])
       ```

原始数据为：286500

清理之后的数据为：278154

### 增加churn列：

增加churn列，创建一列 `Churn` 作为模型的标签。我建议你使用 `Cancellation Confirmation` 事件来定义客户流失，该事件在付费或免费客户身上都有发生。

```
#定义churn 函数，分析Cancellation Confirmation
churn_func = udf(lambda x: 1 if x == "Cancellation Confirmation" else 0, IntegerType())
df = df.withColumn("Churn", churn_func(df.page))
```

## 数据探索分析：

### length 分布：

```
data_len = df.select("length").toPandas()
data_len.plot(kind = "hist", bins = 500)
```



![length](./img/length.png)

从图像上来分析，整体呈现正态分布。

### 付费和未付费的流失情况：

```
plt.figure(figsize=(10,5))
data_level=df.groupby(["churn", "level"]).count().sort("churn").toPandas()
sns.barplot(x='level', y='count', hue='churn', data=data_level)
plt.suptitle('Churned user by subscription-level', fontsize=20)
```



![pay](./img/pay.png)

从分布图上分析，整体在付费的类别中客户流失的较高。



### 性别的流失情况：

```
df_pd = df.dropDuplicates(["userId", "gender"]).groupby(["Churn", "gender"]).count().sort("Churn").toPandas()
sns.barplot(x='Churn', y='count', hue='gender', data=df_pd)
```



![gender](./img/gender.png)



从图中分析，整体上男性的流失和未流失都高于女性。同时也说明了男性使用该服务的人数较多。



### 流失和未流失用户收听的歌曲数量：

```
pd_song = df_churn_user.join(df.groupby("userId") \
                                     .agg({"song": "count"}) \
                                     .withColumnRenamed("count(song)", "song_count"), ["userId"]) \
                       .withColumn("Churn", convert_churn_label("Churn")).toPandas()
sns.boxplot(x="Churn", y="song_count", data=pd_song);
```

![song_count](./img/song_count.png)

从图中分析，整体上非流失用户收听的歌曲总数较多。

###  流失和未流失用户收听的歌手的数量：

```
pd_artist = df_churn_user.join(df.groupby("userId") \
                                    .agg({"artist": "count"}) \
                                    .withColumnRenamed("count(artist)", "artist_count"), ["userId"]) \
                         .withColumn("Churn", convert_churn_label("Churn")).toPandas()
sns.boxplot(x="Churn", y="artist_count", data=pd_artist);
```

![artist](./img/artist.png)

从图中分析，整体上非流失用户收听的歌手的总数也较多。

 ### 流失和未流失用户Thumbs UP的数量：



```
pd_up = df_churn_user.join(df.filter((df["page"] == 'Thumbs Up')) \
                                   .groupby("userId") \
                                   .count() \
                                   .withColumnRenamed("count", "up_count"), ["userId"]) \
                     .withColumn("Churn", convert_churn_label("Churn")).toPandas()
sns.boxplot(x="Churn", y="up_count", data=pd_up);
```



![up](./img/up.png)

从图中分析，整体上非流失用户收听的歌曲总数较多。

### 流失和未流失用户Thumbs Down的数量:

```
pd_down = df_churn_user.join(df.filter((df["page"] == 'Thumbs Down')) \
                                   .groupby("userId") \
                                   .count() \
                                   .withColumnRenamed("count", "down_count"), ["userId"]) \
                     .withColumn("Churn", convert_churn_label("Churn")).toPandas()
sns.boxplot(x="Churn", y="down_count", data=pd_down);
```



![down](./img/down.png)

从图中分析，整体上流失用户和非流失用户在“Thumbs Down”的均值较为接近。

### 流失和未流失用户在不同loaction中的分布:

```
# Plot churned user by level and location
plt.figure(figsize=(50,20))
data_location=df.groupby(["location", "churn"]).count().sort("churn").toPandas()
data_location=sns.barplot(x='location', y='count', hue='churn', data=data_location);
plt.xticks(rotation=90, fontsize=26)
plt.suptitle('Churned user by location', fontsize=60)
```

![location](./img/location.png)



从图中分析，ca Los Angeles-long beach-anaeim地区的流失和未流失用户最高。

## 数据预处理：

对于特征值进行处理，将category的特征（gender，level）进行转换，并对thumps up, thumps down, listening，song进行统计，同时将churn特征重名为label方便后续的训练。

```
def processFeatures(data):
    '''
    处理Gender, Thumps up, Thumps down, Listening time,Number of songs per user, free or paid user等特征
    最终将处理之后的特征整合之后返回
    '''
    # Gender category进行转化
    gender_num = data \
           .select('userId','gender')\
           .dropDuplicates() \
           .replace(['M','F'],['0','1'],'gender')\
           .select('userId',col('gender').cast('int'))\
           .withColumnRenamed('gender', 'gender_num') 
    
    # 统计Thumbs up
    thumbs_up = data \
             .select('userID','page') \
             .where(data.page == 'Thumbs Up') \
             .groupBy('userID') \
             .count() \
             .withColumnRenamed('count', 'thumbs_up') 
    
    # 统计Thumps down
    thumbs_down = data \
                 .select('userID','page') \
                 .where(data.page == 'Thumbs Down') \
                 .groupBy('userID') \
                 .count() \
                 .withColumnRenamed('count', 'thumbs_down')
    
    # 统计Listening Time的总和
    listening_time = data \
            .select('userId','length') \
            .groupby(['userId']) \
            .sum() \
            .withColumnRenamed('sum(length)','listening_time')

    # 计算每位user的song的数量
    song_per_user = data \
                  .select("userId","song")\
                  .groupby("userId")\
                  .count()\
                  .withColumnRenamed("count","song_per_user")
    
    # 将Free or paid category特征进行转换
    level_num = data \
         .select('userId','level')\
         .replace(['free','paid'],['0','1'],'level')\
         .select('userId',col('level').cast('int'))\
         .withColumnRenamed('level', 'level_num') 
    
    # 提取churn并命名为label，在后续的训练的时候需要lable，此处进行命名
    churn = data \
        .select('userId', col('Churn').alias('label')) \
        .dropDuplicates()

    # 整合所有特征
    feature_data = song_per_user.join(gender_num,'userID','outer') \
              .join(thumbs_up,'userID','outer') \
              .join(thumbs_down, 'userID','outer') \
              .join(listening_time,'userID','outer') \
              .join(level_num,'userID','outer') \
              .join(churn,'userID','outer') \
              .drop('userID') \
              .fillna(0)


    
    return feature_data
```



## 模型搭建：

###  F1 score 作为优化指标

由于流失客户chrun的数据集比较少，因此在模型中采用F1 score作为优化指标。

F1值 = 正确率 * 召回率 * 2 / (正确率 + 召回率)

![1554072551600](./img/f1.png)



F1 score是综合考虑了模型查准率和查全率的计算结果，取值更偏向于取值较小的那个指标。F1-score越大自然说明模型质量更高。但是还要考虑模型的泛化能力，F1-score过高但不能造成过拟合，影响模型的泛化能力。

### 模型选择
在分类问题中我们采用如下的模型进行分析
- LogisticRegression
采用该模型的原因是，该模型可以处理因变量为分类变量的回归问题，该算法的优点是容易理解与实现，计算代价不高。对于流失用户的分类：流失用户和非流失用户两种类型，采用该模型；
- GBTClassifier
GBDT算法，在计算损失函数在当前模型的负梯度值，作为下一次模型训练的目标函数，每次迭代时沿着损失函数的负梯度方向移动，最终损失函数越来越小，得到越来越精确的模型。同时该模型只处理label为0和1的场景，满足项目的需求
- RandomForestClassifier
随机森林是一个元估计器，它适合数据集的各个子样本上的多个决策树分类器，并使用平均值来提高预测精度和控制过度拟合。该项目中churn为0,1的分类问题，该模型适合该项目



### 提取训练和测试数据：

```
# 创建训练和测试数据
train, validation = df.randomSplit([0.8, 0.2], seed=42)
```

### 建立模型：

```
logist = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0)
gbt=GBTClassifier()
forest = RandomForestClassifier()
```

### 训练模型：

```
    ##使用f1 score指标
    f_score = MulticlassClassificationEvaluator(metricName='f1')
    model_train = model.fit(train)
    model_test = model_train.transform(validation)
    #结果评估
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction')
```



### 结果分析：

- LogisticRegression

```
Accuracy is:
0.8368801987684995
F-1 score is :
0.7625630322111933
Running 190.6146936416626 seconds 
```

- GBTClassifier

```
Accuracy is:
0.9898815310935868
F-1 score is :
0.9897499951607381
Running 609.903459072113 seconds 
```

- RandomForestClassifier

```
Accuracy is:
0.8818011594829138
F-1 score is :
0.8521219246862239
Running 256.87881445884705 seconds 
```



F1 score:

 GBTClassifier：0.9897499951607381 

LogisticRegression:0.7964339801776644 

RandomForestClassifier:0.7964339801776644 

从结果分析GBTClassifier可能有点过拟合，进一步对GBTClassifier超参进行调节。




## 超参调试：

使用CrossValidato来获取最优的参数。

交叉验证CrossValidato将数据集切分成k折叠数据集合，并被分别用于训练和测试。例如，k=3时，CrossValidator会生成3个（训练数据，测试数据）对，每一个数据对的训练数据占2/3，测试数据占1/3。为了评估一个ParamMap，CrossValidator 会计算这3个不同的（训练，测试）数据集对在Estimator拟合出的模型上的平均评估指标。在找出最好的ParamMap后，CrossValidator 会使用这个ParamMap和整个的数据集来重新拟合Estimator。

### 参数范围：

- maxDepth
  树的最大深度，0意味着只有一个叶节点，1意味着有一个内部节点+两个叶节点。
  范围：[5,10]

- maxIter
  最大迭代次数

  范围:[5,10]

### 模型建立：

```
gbt=GBTClassifier()
```

### 模型的训练：

具体代码如下：

```
    f1_score = MulticlassClassificationEvaluator(metricName='f1')
    param_gbt = ParamGridBuilder()\
       .addGrid(gbt.maxIter,[5,10])\
       .addGrid(gbt.maxDepth,[5,10]) \
       .build()
    crossval_model = CrossValidator(estimator=model,
                           evaluator=f1_score,
                           estimatorParamMaps=param_gbt,
                           numFolds=3)
    model_train = crossval_model.fit(train)
    model_test = model_train.transform(validation)
```

### 结果分析：

```
Model is: GBTClassifier_2728bb659038
Accuracy:
0.99641712577869
F-1 score:
0.9964010597229603
Running 3677.6525661945343 seconds 
```

通过交叉验证，整体的准确率和F1 score有了一定的提升。但是整体训练的过程花费的时间成本较大。



## 结论：

通过GBTClassifier建立模型对Sparkify音乐服务数据集中的客户流失情况进行预测和分析，该模型能过很好的对于流失用户的情况进行预测，对应的F1 score能够达到0.9964010597229603，已经能够很好的对数据集中的数据进行预测，从而能够帮忙公司进行相应的调整从而达到一个较高的客户保持率。

## 总结：

- 通过CrossValidator进行处理，在较小的数据集中可以比较快速的得到较为优化的参数，但是对于数据量庞大的时候会花费很多时间；
- 调参数在时候需要平衡精度和召回率，不能只针对精度进行优化；
- 在数据清洗过程中，需要对userId和sessionId为空的数据项过滤掉。
- 在数据探索阶段，为了方便后续进行训练，提前增加标签，以便标记哪些是流失用户。
- 通过最终对于多个模型的对比，发现GBTClassifier是该项目最佳模型。



## 待改进：

- 在进行训练的时候应该使用pipeline进行处理，这样整体的逻辑和代码比较简单明了
- 尽可能将每一步的操作都概括为单个的函数

## 参考：

- https://spark.apache.org/docs/2.0.1/api/java/overview-summary.html
- https://stackoverflow.com/questions/37152723/how-to-auto-discover-a-lagging-of-time-series-data-in-scikit-learn-and-classify/37214127#37214127
- https://www.kaggle.com/fatmakursun/pyspark-ml-tutorial-for-beginners
- https://www.kaggle.com/c/predicting-red-hat-business-value/discussion/23777#136110