import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

appName = 'myProz1'

current = os.getcwd()

src_olap = current+'\source\src_olap'
src_oltp = current+'\source\src_oltp'
