import os

import moment
import pydash
from jinja2 import Template

from utils.constant import PROJECT_PATH, BUSINESS_TYPE_WITH_SECOND_TYPE


class BaseTemplate:
	temp_path = f'{PROJECT_PATH}/utils/template/base/'
	sql_temp_path = temp_path + 'sql_template.txt'
	dags_temp_path = temp_path + 'dags_template.txt'
	init_temp_path = temp_path + 'init_template.txt'

	@staticmethod
	def upper_format(name: str) -> str:
		return pydash.camel_case(name).capitalize()

	def __init__(
			self, project, business_type, chain='ethereum',
			sql_temp_paths: dict = None, dags_temp_paths: dict = None,
			init_temp_paths: dict = None
	):
		assert business_type in BUSINESS_TYPE_WITH_SECOND_TYPE.keys(), 'business_type not in BUSINESS_TYPE_WITH_SECOND_TYPE.keys()'
		self.temp_type = business_type
		self.business_type_upper = pydash.camel_case(business_type).capitalize()
		self.business_second_types = BUSINESS_TYPE_WITH_SECOND_TYPE[business_type]
		self.chain = chain
		self.project = project
		self.project_path = f'{PROJECT_PATH}/{self.temp_type}/{chain}/{project}'
		self.business_second_format_types = [{
			'type': second_type, 'type_upper': self.upper_format(second_type)} for second_type in self.business_second_types]
		self.sql_temp_paths = sql_temp_paths if sql_temp_paths and isinstance(sql_temp_paths, dict) else {}
		# self.dags_temp_paths = dags_temp_paths if dags_temp_paths and isinstance(dags_temp_paths, dict) else {}
		# self.init_temp_paths = init_temp_paths if init_temp_paths and isinstance(init_temp_paths, dict) else {}

	def gen_template(self):
		self.gen_folder()  # 创建 项目文件夹
		self.gen_dags()  # 生成 dag创建文件
		self.gen_init()  # 生成 __init__.py
		self.gen_sql()  # 生成 sql
		print('done')
		pass

	def gen_folder(self):
		if not os.path.exists(self.project_path):
			os.makedirs(self.project_path)

	def gen_dags(self):
		with open(self.dags_temp_path) as template_file:
			template = Template(template_file.read())
			for business_second_type in self.business_second_format_types:
				output_file_path = os.path.join(
					self.project_path,
					f'{self.project}_{self.temp_type}_{business_second_type["type"]}.py')
				with open(output_file_path, 'w') as output_file:
					dags = template.render(
						project=self.project,
						project_upper=self.upper_format(self.project),
						business_type=self.temp_type,
						business_type_upper=self.business_type_upper,
						business_second_type=business_second_type,
						chain=self.chain,
						date="'{}'".format(moment.utcnow().datetime.strftime("%Y-%m-%d")))
					output_file.write(dags)

	def gen_init(self):
		init_template_file_path = os.path.join(self.project_path, '__init__.py')
		with open(self.init_temp_path) as template_file, open(init_template_file_path, 'w') as output_file:
			template = Template(template_file.read())
			init = template.render(
				project=self.project,
				project_upper=pydash.camel_case(self.project).capitalize(),
				chain=self.chain,
				business_type=self.temp_type,
				business_type_upper=self.business_type_upper,
				business_second_types=self.business_second_format_types)
			output_file.write(init)

	def gen_sql(self):
		for business_second_type in self.business_second_types:
			sql_path = self.sql_temp_paths.get(business_second_type, self.sql_temp_path)
			with open(sql_path) as template_file:
				template = Template(template_file.read())
				sql = template.render(business_second_type=business_second_type)
				out_file_path = os.path.join(
					self.project_path,
					f'{self.project}_{self.temp_type}_{business_second_type}.sql')
				with open(out_file_path, 'w') as out_file:
					out_file.write(sql)


if __name__ == '__main__':
	# """执行demo"""
	BaseTemplate(project='cream2', business_type='lending', chain='ethereum').gen_template()
