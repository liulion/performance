1. 执行sql_script文件夹中的sql脚本
2. 更改配置文件中文件夹路径
3. 将估值表放入assets文件夹中，将净值数据表放入performance文件中下的src文件夹
3. 使用Python 2.7 执行
3.1 先执行assets.py从估值表中读取投资组合资产信息
3.2 再执行performance.py选择从Excel中还是Oracle中获取需要计算净值的数据来源
4. 执行后的结果在performance文件中下的des文件夹
5.1  ./Lib/site-packages/xlrd/formula.py
5.2 func_defs字典中键为184和186中间添加
	186行左右，在文字184和文字189中间加插入一行
    186: ('HACKED', 1, 1, 0x02, 1, 'V', 'V'),
    (截图)