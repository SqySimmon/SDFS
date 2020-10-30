comps=(thumm01 thumm02 thumm03 thumm04 thumm05)
for comp in ${comps[@]}
do
	# ssh ${comp} "mkdir ~/sqy_dfs"
	# ssh ${comp} "mkdir /home/dsjxtjc/2018211149/mr_job"

	# scp -r /home/dsjxtjc/2018211149/dfs_sqy ${comp}:/home/dsjxtjc/2018211149/
	scp -r /home/dsjxtjc/2018211149/mapreduce ${comp}:/home/dsjxtjc/2018211149/
	# scp -r /home/dsjxtjc/2018211149/utils ${comp}:/home/dsjxtjc/2018211149/
	# scp -r /home/dsjxtjc/2018211149/client ${comp}:/home/dsjxtjc/2018211149/
	# scp -r /home/dsjxtjc/2018211149/job_statistic ${comp}:/home/dsjxtjc/2018211149/
	# ssh ${comp} "scp ~/yadoop/yadoop_setting.xml"
	# scp ~/yadoop/yadoop_setting.xml 2018211149@${comp}:~/yadoop
	# ssh ${comp} "mv ~/data/amazon_book/splited_amazon_reviews_us_Books* ~/data/amazon_book/amazon_book_splited"
	# scp ${data_path} 2018211149@${comp}:~/data/amazon_book
	# ssh ${comp} "time awk -F "\t" 'BEGIN{sum = 0.0}{sum+=$8}END{print sum}' ${data_path} >> muti_point.txt"
	# ssh -T ${comp} < multi_count.sh &
	# ssh ${comp} "ls  ${data_folder}"
done
wait

# python dfs_sqy/dfs.py init

# python dfs_sqy/dfs.py add_dfs_node_ip thumm02

# python dfs_sqy/dfs.py add_dfs_node_ip thumm03

# python dfs_sqy/dfs.py add_dfs_node_ip thumm04

# python dfs_sqy/dfs.py add_dfs_node_ip thumm05

# python dfs_sqy/dfs.py mkdir /test_dir1/test_dir2

# python dfs_sqy/dfs.py copyFromLocal /home/dsjxtjc/2018211149/data/wordcount/test/aa /test_dir1/cpfromlocal.txt

# python dfs_sqy/dfs.py copyToLocal /test_dir1/cpfromlocal.txt /home/dsjxtjc/2018211149/data/wordcount/test/cpfromlocal.txt

# python dfs_sqy/dfs.py gfile /test_dir1/cpfromlocal.txt

# python dfs_sqy/dfs.py delete /test_dir1/cpfromlocal.txt

# python dfs_sqy/dfs.py ls /test_dir1

# python /home/dsjxtjc/2018211149/mapreduce/map_reduce.py submit /home/dsjxtjc/2018211149/job_statistic/mean_variance.py /test_dir1/cpfromlocal.txt /test_dir1/result_node2.txt

# # python /home/dsjxtjc/2018211149/job_statistic/mean_variance.py map /home/dsjxtjc/2018211149/dfs_data/2380270681 1
# # python mapreduce/map_reduce.py init
# # python /home/dsjxtjc/2018211149/job_statistic/mean_variance.py map /home/dsjxtjc/2018211149/data/wordcount/test/test_little.txt 1

# # python /home/dsjxtjc/2018211149/job_statistic/mean_variance.py reduce /home/dsjxtjc/2018211149/mr_inter_data/0 1


# # client code :
# python /home/dsjxtjc/2018211149/client/dfs_client.py ls /test_dir1

# python /home/dsjxtjc/2018211149/client/dfs_client.py gfile /test_dir1/cpfromlocal.txt

# python /home/dsjxtjc/2018211149/client/dfs_client.py mkdir /test_dir1/test_dir3

# python /home/dsjxtjc/2018211149/client/dfs_client.py touch /test_dir1/test_dir3/test_file2.txt

# python /home/dsjxtjc/2018211149/client/dfs_client.py copyFromLocal /home/dsjxtjc/2018211149/count.txt /test_dir1/test_dir3/count.txt

# python /home/dsjxtjc/2018211149/client/dfs_client.py copyToLocal /test_dir1/test_dir3/count.txt /home/dsjxtjc/2018211149/count2.txt

# python /home/dsjxtjc/2018211149/client/dfs_client.py rm /test_dir1/test_dir3/count.txt


