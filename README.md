Python multi-process version reads kafka and writes to mongo, works and configures like logstash

  * 简易配置: 参考logstash, pipeline input filter output的形式, 详见例子
  * 输入输出: 输入kafka, 输出mongodb, 其他case建议使用logstash, 也可补充读写函数扩展
  * filter: 过滤层, etl脚本, 可使用任意语言/管道等方式处理输入数据, 默认仅支持python脚本, 可扩展

  * 自动load: 自动检测配置文件的变更, 增加/重启/移除任务, 间隔5s
  * 自动bulk: 大于bulk阀值1000或60s后bulk
  * 安全退出: 退出时会等待清理各bulk队列中的数据, 避免强制退出

  * 多向扩展: 横向任务并发, 纵向多任务, 每个任务支持多个output(未测试); 暂不支持多input(多input请使用logstash);
  * 轻量镜像: 仅依赖python pymongo kafka-python pyyaml等包


```
default_example:
  pipeline:
    id: "example_to_mongo_1"
    workers: 2
    # don's set the value too large
    batch.size: 500
    alert: "johnny"
  input:
    kafka:
      bootstrap_servers: "127.0.0.1:9092"
      topics: ["test_topic"]
      group_id: "test_2_mongo"
      auto_offset_reset: "latest"
  filter:
    python:
      script: "scripts/transform_quake.py"
  output:
    - mongodb:
        uri: "mongodb://mongo:password@127.0.0.1:30079"
        database: "db"
        collection: "test"
        readPreference: "secondaryPreferred"
        maxPoolSize: 100
        serverSelectionTimeoutMS: 500000
        socketTimeoutMS: 600000
```

