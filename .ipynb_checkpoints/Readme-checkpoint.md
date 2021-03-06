
## 项目：预测用户流失

## 项目概况
Sparkify这是一个虚拟的音乐服务数据集，拥有过千万用户，用户可以随时升级、降级、取消他们的套餐。用户的动态、意向可以直接影响到服务的盈利；而每次用户的操作都会被记录(即具体动作例如收藏、点赞、差评、播放歌曲、播放时长等)，这些数据对于服务商而言有着重要价值，可从该数据中发现某些用户的某些操作的共通点，来判断该用户接下来会进行什么样的操作， 项目的目标是寻找潜在客户，而潜在客户也分为潜在意向客户和流失客户。利用Spark操纵大的真实数据集来抽取相关性特征来预测客户流失情况,使用 Spark MLib 搭建机器学习模型处理大数据集，利用机器学习找到那些流失的客户。

## 项目的目
项目的目的是通过对于Sparkify音乐服务数据集各个特征的分析，建立模型来预测用户流失的情况。
通过如下的步骤来进行处理：
- 对于提供的数据集中的数据进行分析，对数据进行清理，修复或者删除异常值，删除不能为任务提供有用价值的数据。对数据集中各个特征值的关系进行可视化的分析；
- 研究各种机器学习的模型，选择合理的模型进行训练，对于用户流失的情况进行预测和评估；


## 环境&依赖库
Python3.7
PySpark 分布式机器学习库
matplotlib 可视化库
numpy 科学计算库
pandas 
本地使用anaconda里安装和使用


## 目录结果：
Readme.md
Sparkify-zh.ipynb notebook的代码部分
Sparkify-zh.html
SparklyReport.md 最终的报告
img/  分析过程中的各个特征的关系图

## 结果：
通过GBTClassifier建立模型对Sparkify音乐服务数据集中的客户流失情况进行预测和分析，该模型能过很好的对于流失用户的情况进行预测，对应的F1 score能够达到0.9964010597229603，已经能够很好的对数据集中的数据进行预测，从而能够帮忙公司进行相应的调整从而达到一个较高的客户保持率。
