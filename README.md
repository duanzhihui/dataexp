# dataexp
dataexp 数据探查工具,参考 talend 公司 data quality 产品,精选12个常用指标，通过ai编程目前支持 oracle， sql server， mysql， impala 四种国内常用数据库类型。采用工厂模式编程，可以扩展数据库类型。可选 ssh 隧道连接数据库。

## 部署
1. 安装依赖
```bash
pip install -r requirements.txt
```
2. 配置文件
```yaml
config.yml
密码中含有特殊字符，需要转义，比如：
@ 编码为 %40
/ 编码为 %2F
: 编码为 %3A
? 编码为 %3F
& 编码为 %26
```
3. 运行
```bash
python dataexp.py
python dataexp.py --config config.yml
```

## roadmap
version 1.*    mysql版本
version 0.*    excel版本
version 0.5    excel版本，支持通过隧道连接数据库
version 0.4    excel版本，支持多数据库切换，不拆分sheet
version 0.3    excel版本，减少excel公式计算，由python直接计算。