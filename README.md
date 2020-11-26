# Camel Open Server

Analyze open server data


话说开服是一个大事儿，很多东西都可以用到这个里面的， 如果大家谁有想法啥的，随时再这个里面加东西。
目前写了一个etl， 这个模块就是主要从数据库里面取东西的。以后会陆续加一些transform模块， aggregation模块
analyze模块，model模块什么的。

但是基本的要求啊，就是
1. 不要再etl里面加入一些pandas dataframe的操作，简单的修改列名可以，转换不行，要写其他模块。
2. 不要折腾出来字段是固定的东西，对于每个表，可以灵活选择字段，以后可以根据配置再不同的游戏运行。
3. 不要拼SQL，很难受，以后就没法看懂了。
4. 不要使用老版本的Camel_v2，一律使用dbx, camel_queries, camel_utils_x, 这些东西。

我琢磨config里面的东西配置好了以后，在不同的游戏运行不同的config
