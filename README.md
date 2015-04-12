# tianchi_contest
The code for tianchi recommenda contest




##数据导入sqlite

请创建和tianchi_contest一级的data目录

从官方下载的文件解压后有两个文件：tianchi_mobile_recommend_train_user.csv和tianchi_mobile_recommend_train_item.csv，第一个即为用户历史，第二个是item的信息。需要运行两个脚本将数据导入sqlite里。

	python preprocessing_train_data.py ../data/tianchi_mobile_recommend_train_user.csv
	
这个步骤是将用户记录增加一个weekday的信息，放在最后一列；

然后就可以直接运行下面代码，会创建sqlite的表，并将user和item的信息分别导入到表中，对应的文件都存在../data/目录下。（）
	
	python dump_data.py 0
	
一切搞定后，就可以装一个sqlite browser，打开data/tianchi.db文件查看其中的数据了。
	
	 
