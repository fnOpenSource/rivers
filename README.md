# Rivers是什么?
    Rivers是一个支持在任意类型数据源之间进行可定时定量交换数据的中间件。 


![image](https://github.com/fnOpenSource/rivers/blob/master/flowchart.png)

# Rivers用来解决什么?

目前成熟的数据交换工具比较多，但是一般都只能用于数据导入或者导出，并且只能支持一个或者几个特定类型的数据库。

这样带来的一个问题是，如果我们拥有很多不同类型的数据库/文件系统(Mysql/Oracle/Rac/Hive/Other…)/消息/http读写等，可以对不同的数据源有小范围的搜索需求等，并且经常需要在它们之间交换数据，那么我们可能需要开发/维护/学习使用一批这样的工具(jdbcdump/dbloader/multithread/getmerge+sqlloader/mysqldumper…)。

而且以后每增加一种库类型，我们需要掌握和部署的工具数量将线性增长。数据交换的任务中常见的需求，比如定时增量全量导入、数据简单转化、数据简单检索需求、分布式数据处理等。

Rivers正是为了解决这些问题而生。 
