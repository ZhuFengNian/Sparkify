# 内容： Sparkify
## 项目：预测用户流失

## 项目概况
Sparkify这是一个虚拟的音乐服务数据集，拥有过千万用户，用户可以随时升级、降级、取消他们的套餐。用户的动态、意向可以直接影响到服务的盈利；而每次用户的操作都会被记录(即具体动作例如收藏、点赞、差评、播放歌曲、播放时长等)，这些数据对于服务商而言有着重要价值，可从该数据中发现某些用户的某些操作的共通点，来判断该用户接下来会进行什么样的操作， 项目的目标是寻找潜在客户，而潜在客户也分为潜在意向客户和流失客户。利用Spark操纵大的真实数据集来抽取相关性特征来预测客户流失情况,使用 Spark MLib 搭建机器学习模型处理大数据集，利用机器学习找到那些流失的客户。

## 项目输出
Sparkify-zh为SparkifyNotebook的子集，是一个小数据集，主要用来展示可视化；没有扩展大数据集进行处理

## 环境&依赖库
Python
PySpark 分布式机器学习库
matplotlib 可视化库
numpy 科学计算库
pandas 
本地使用anaconda里安装和使用


## 总结：
- 通过CrossValidator进行处理，在较小的数据集中可以比较快速的得到较为优化的参数，但是对于数据量庞大的时候会花费很多时间；
- 调参数在时候需要平衡精度和召回率，不能只针对精度进行优化；
- 在数据清洗过程中，需要对userId和sessionId为空的数据项过滤掉。
- 在数据探索阶段，为了方便后续进行训练，提前增加标签，以便标记哪些是流失用户。
- 通过最终对于多个模型的对比，发现GBTClassifier是该项目最佳模型。


## 改进
- 在进行训练的时候应该使用pipeline进行处理，这样整体的逻辑和代码比较简单明了
- 尽可能将每一步的操作都概括为单个的函数

## 参考
- https://spark.apache.org/docs/2.0.1/api/java/overview-summary.html
- https://stackoverflow.com/questions/37152723/how-to-auto-discover-a-lagging-of-time-series-data-in-scikit-learn-and-classify/37214127#37214127
- https://www.kaggle.com/fatmakursun/pyspark-ml-tutorial-for-beginners
- https://www.kaggle.com/c/predicting-red-hat-business-value/discussion/23777#136110

