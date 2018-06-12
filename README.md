# auto-complete
自动补全，练手项目，使用带平滑算法的ngram模型
####安装
配置hadoop_home变量和path(hadoop3.1.0),按照pom.xml安装依赖
####快速开始
参数input/ output/ 2 0.75
#### 平滑算法
简单实现了Kneser-Ney Smoothing,参考https://lagunita.stanford.edu/c4x/Engineering/CS-224N/asset/slp4.pdf,
https://www3.nd.edu/~dchiang/papers/zhang-acl14.pdf
####算法流程

####todo
http://u.cs.biu.ac.il/~yogo/courses/mt2013/papers/chen-goodman-99.pdf，
modified kn,best-performing version of kn smoothing,adaptive discount?
速度优化等