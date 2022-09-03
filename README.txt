# ls

就在前几天的晚上，突然想起了做一个我自己都不知道的东西，那么来吧。可能就是灵感来了，那就做一个自己想要的东西咯。"快搜"搜索引擎。
说搜索引擎有点太过于耍大牌，引擎不引擎，还不是钱说了算，我这顶多就是一个内网查询器。项目2天做完了，还差点东西，就是这个文档。

执行下面命令：
python3 setup.py bdist_wheel

之后会生成下面3个命令:
1. kuaiso-web 快搜web服务接口，默认地址localhost:9999
2. kuaiso-db 用于初始化搜索引擎，kuaiso-db --command init_full_index
3. kuaiso-spider 用于启动爬虫任务，输入kuaiso-spider --help查看命令详细

项目启动，需要依赖，2个中间件：
1. ElasticSearch最新版本就行，如果有问题，可以联系我
2. RabbitMQ最新版本，现已发现如果unack数等于`channel.basic_qos(prefetch_count=10)`中的`prefetch_count`，那么
消费者会阻塞住

接下来需要记录一下，es的配置，主要说配置，我是将elasticsearch.yml中的这个选项改为false再重启ElasticSearch就不用输入账号
密码，同时，也不需要走ssl通道，直接http就可以：
xpack.security.enabled: false

然后再说下RabbitMQ的配置，我装的默认就应安装了web界面的插件，安装后的默认账号密码为:guest/guest