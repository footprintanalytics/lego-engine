from google.cloud import storage
from config import project_config

storage_client = storage.Client()


class GCSUtil:
	def __init__(self, bucket_name: str = None, user_project=None):
		self.bucket_name = bucket_name or project_config.bigquery_bucket_name
		self.bucket = storage_client.bucket(self.bucket_name, user_project=user_project)

	def upload_file(self, source_file_name, destination_blob_name=None, destination_blob=None):
		if destination_blob is None:
			destination_blob = self.bucket.blob(destination_blob_name)
		destination_blob.upload_from_filename(source_file_name)

		print(
			"File {} uploaded to {}.".format(
				source_file_name, destination_blob_name
			)
		)

	def upload_file_with_cache(self, source_file_name, destination_blob_name):
		blob = self.bucket.blob(destination_blob_name)
		if not blob.exists():
			print('file:{} exits skip'.format(destination_blob_name))
		self.upload_file(source_file_name, destination_blob_name, destination_blob=blob)

	# def delete_file(self, destination_file_name):
	# 	blob = self.bucket.blob(destination_file_name)
	# 	if blob.exists():
	# 		blob.delete()
	# 		print("Blob {} deleted.".format(destination_file_name))
	# 	else:
	# 		print("Blob {} not exists.".format(destination_file_name))

	def move_file(self, source_blob_name, destination_blob_name, source_blob=None):
		if not source_blob:
			source_blob = self.bucket.blob(source_blob_name)
		if source_blob.exists():
			new_blob = self.bucket.rename_blob(source_blob, destination_blob_name)
			print("Blob {} has been renamed to {}".format(source_blob.name, new_blob.name))

	def download_file(self, source_blob_name, destination_file_name, source_blob=None):
		"""Downloads a blob from the bucket."""
		if not source_blob:
			source_blob = self.bucket.blob(source_blob_name)
		source_blob.download_to_filename(destination_file_name)

		print(
			"Downloaded storage object {} from bucket {} to local file {}.".format(
				source_blob_name, self.bucket_name, destination_file_name
			)
		)

	def list_dir(self, bob_path, return_blob=False, return_dir=False, delimiter=None, max_results=10000):
		"""列出目录下的所有文件, 注意, 这里为了减轻项目开销, 默认限制了最大返回文件数"""
		blobs = storage_client.list_blobs(
			self.bucket_name,
			prefix=bob_path,
			max_results=max_results,
			delimiter=delimiter or ('/' if return_dir else None)
		)
		if return_dir:
			return [prefix for prefix in blobs.prefixes]
		if return_blob:
			return blobs
		return [b.name for b in blobs]


# if __name__ == '__main__':
	# 实例化
	# gsc_util = GCSUtil('test')

	# 移动文件夹文件
	# blobs = gsc_util.list_dir(
	# 	'data/ethereum_dex_dodo/dodo_dex_swap',
	# 	delimiter='/',
	# 	return_blob=False,
	# 	max_results=1000)
	# for blob in blobs:
	# 	gsc_util.move_file(blob.name, f'{blob.name}.txt', blob)

	# gsc_util.upload_file(source_file_name='/Users/pen/trans_address', destination_file_name='export/test/test.txt')
	# gsc_util.delete_file(destination_file_name='export/test/test.txt')
	# gsc_util.move_file('export/test.txt', 'export/test/test.txt')
	# gsc_util.list_dir('export/test.txt', 'export/test/test.txt')
