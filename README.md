# auto-complete
简单实现了带Kneser–Ney平滑的ngram模型，用于对用户输入进行自动补全。模型可能不太适用于自动补全，主要为了学习。[web页面项目](https://github.com/walio/ac-web)
####安装
配置hadoop_home变量和path(hadoop3.1.0),按照pom.xml安装依赖
####快速开始
参数input/ output/ 2 5 0.75
#### 平滑算法
简单实现了Kneser-Ney Smoothing，主要参考了[这篇文章](https://lagunita.stanford.edu/c4x/Engineering/CS-224N/asset/slp4.pdf),
和[wiki](https://en.wikipedia.org/wiki/Kneser%E2%80%93Ney_smoothing)
标准kn模型空间太大，对于每个在语料库中出现的单词组合，会计算语料库中所有单词出现在其后的概率，测试40M的数据计算到2gram时能达到5G
####算法流程
公式：
1. 提取词频，低阶直接wordcount，最高阶使用continuation
2. 计算公式中的高阶概率和lambda，并将1阶的概率乘以lambda插入二阶，将插值后的二阶概率乘以lambda插入三阶，依次类推
3. 分别将上述插值后的一阶、二阶、三阶模型存入hbase数据库，包括概率和lambda

计算时，对于input，分两种情况：
1. 数据库中存在此单词组合，比较高阶模型和低阶模型*lambda，取topk，或者按照总概率取排名靠前的单词
2. 数据库中不存在此单词组合，理论上来说回退到低阶模型，但使用高阶模型中的低阶概率代替（低阶模型的最高阶概率应当使用正常的计数方法，但高阶模型的低阶概率使用的是continuation count）
####todo
http://u.cs.biu.ac.il/~yogo/courses/mt2013/papers/chen-goodman-99.pdf

modified kn,best-performing version of kn smoothing,adaptive discount?
速度优化等