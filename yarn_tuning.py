
import io
import json
import openpyxl
import pandas as pd
import requests
import sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import xmljson as xml
import xmltodict
import zipfile


def load_config():
    with open('config.json') as json_data_file:
        config = json.load(json_data_file)
    return config


# Get Credentials
config = load_config()

cloudera_manager_username = config['cloudera_manager']['username']
cloudera_manager_password = config['cloudera_manager']['password']
cloudera_manager_url = config['cloudera_manager']['url']
cloudera_manager_port = config['cloudera_manager']['port']
cluster_name = config['cluster']['name']

response = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/clusters/{cluster_name}/services/yarn/roles/',
                        auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)

json_data = response.json()

# Get all the hosts with YARN Nodemanager
hosts = []
for entry in json_data['items']:
    if (entry['type'] == "NODEMANAGER"):
        print(entry['hostRef']['hostId'])
        hosts.append(entry['hostRef']['hostId'])

host_details = []
response = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/hosts',
                        auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)
json_data = response.json()

for host in hosts:

    for entry in json_data['items']:
        if (entry['hostId'] == host):
            ans = {"hostId": host, "hostName": entry['hostname'],
                   "numCores": entry['numCores'], "mem": int(entry['totalPhysMemBytes']/1024/1024/1024)}
            host_details.append(ans)

df = pd.DataFrame(host_details)

for cores, cnt in df.numCores.value_counts().iteritems():
    xfile = openpyxl.load_workbook('yarn-tuning-guide.xlsx')
    # Cluster Configuration
    sheet = xfile['Cluster Configuration']
    print('value', cores, 'was found', cnt, 'times')
    sub_df = df[df['numCores'] == cores]
    sheet['D8'] = int(sub_df["mem"].mean())
    sheet['F9'] = int(sub_df["numCores"].mean())
    sheet['D37'] = int(sub_df.shape[0])

    # xfile.save(f'yarn-tuning-guide-{cores}.xlsx')
    # xfile = openpyxl.load_workbook(f'yarn-tuning-guide-{cores}.xlsx')
    sheet = xfile['Cluster Configuration']
    

    # HDFS DATANODE
    r = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/clusters/{cluster_name}/services/hdfs/roleConfigGroups',
                     auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)

    json_data = r.json()


    for entry in json_data['items']:
        if (entry['roleType'] == "DATANODE"):
            

            for x in (entry['config']['items']):
                if (x['name'] == "datanode_java_heapsize"):
                    print(int(x['value'])/1024/1024)
                    sheet['F21'] = int(x['value'])/1024/1024
                    break

    # HDFS DATANODE
    r = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/clusters/{cluster_name}/services/yarn/roleConfigGroups',
                     auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)

    json_data = r.json()
    

    for entry in json_data['items']:
        if (entry['roleType'] == "NODEMANAGER"):
            print(entry['config']['items'])

            for item in (entry['config']['items']):
                if (item['name'] == "node_manager_java_heapsize"):
                    print(int(item['value'])/1024/1024)
                    sheet['F22'] = int(item['value'])/1024/1024
                    break

    # IMPALA
    r = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/clusters/{cluster_name}/services/impala/roleConfigGroups',
                     auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)

    json_data = r.json()
    print(json_data)

    for entry in json_data['items']:
        if (entry['roleType'] == "IMPALAD"):
            print(entry['name'])
            r = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/clusters/{cluster_name}/services/impala/roleConfigGroups/{entry["name"]}',
                             auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)

            json_data = r.json()

            impala_memory = 0
            for item in json_data['config']['items']:
                if (item['name'] == 'impalad_memory_limit'):
                    impala_memory += (int(item['value'])/1024/1024)
                if (item['name'] == 'impalad_embedded_jvm_heapsize'):
                    impala_memory += (int(item['value'])/1024/1024)

    sheet['F23'] = int(impala_memory)
    
    # YARN Configuration
    r = requests.get(f'{cloudera_manager_url}:{cloudera_manager_port}/api/v32/clusters/{cluster_name}/services/yarn/clientConfig',
                     auth=(f'{cloudera_manager_username}', f'{cloudera_manager_password}'), verify=False)

    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("tmp")

    with open("tmp/yarn-conf/yarn-site.xml") as xml_file:

        data_dict = xmltodict.parse(xml_file.read())
        data_dict = dict(data_dict)
        # print(data_dict['Configuration'])
        for key, value in data_dict.items():
            print(key)
        json_data = json.dumps(data_dict)
        json_data = json.loads(json_data)
        xml_file.close()

        yarn_configs = {'yarn.scheduler.minimum-allocation-vcores': 'F22', 'yarn.scheduler.maximum-allocation-vcores': 'F23', 'yarn.scheduler.increment-allocation-vcores': 'F24',
                        'yarn.scheduler.minimum-allocation-mb': 'F27', 'yarn.scheduler.maximum-allocation-mb': 'F28', 'yarn.scheduler.increment-allocation-mb': 'F29'}

        yarn_df = pd.DataFrame(json_data['configuration']['property'])
        print(yarn_df.head())

        sheet = xfile['YARN Configuration']

        prop = []
        values = []
        print(values)
        for val, cell in yarn_configs.items():
            prop.append(val)
            print(
                f"{val}: {yarn_df.loc[yarn_df['name'] == val, 'value'].iloc[0]}")
            input = yarn_df.loc[yarn_df['name'] == val, 'value'].iloc[0]
            try:
                sheet[cell] = float(input)
                values.append(input)
            except Exception:
                values.append(None)
                continue

        values = [int(i) for i in values]
        yarn_properties = dict(zip(prop, values))
        print(yarn_properties)

        # MapReduce Configuration

        with open("tmp/yarn-conf/mapred-site.xml") as xml_file:
            data_dict = xmltodict.parse(xml_file.read())
            data_dict = dict(data_dict)
            # print(data_dict['Configuration'])
            for key, value in data_dict.items():
                print(key)
            json_data = json.dumps(data_dict)
            json_data = json.loads(json_data)

            xml_file.close()

            mr_configs = {'yarn.app.mapreduce.am.resource.cpu-vcores': 'F8', 'yarn.app.mapreduce.am.resource.mb': 'F9', 'yarn.app.mapreduce.am.command-opts': 'F10', 'mapreduce.job.heap.memory-mb.ratio': 'F13', 'mapreduce.map.cpu.vcores': 'F15',
                          'mapreduce.map.memory.mb': 'F16', 'mapreduce.map.java.opts': 'F17', 'mapreduce.task.io.sort.mb': 'F18', 'mapreduce.reduce.cpu.vcores': 'F20', 'mapreduce.reduce.memory.mb': 'F21', 'mapreduce.reduce.java.opts': 'F22'}

            map_df = pd.DataFrame(json_data['configuration']['property'])

            sheet = xfile['MapReduce Configuration']

            for val, cell in mr_configs.items():
                print(
                    f"{val}: {map_df.loc[map_df['name'] == val, 'value'].iloc[0]}")
                if (val == "yarn.app.mapreduce.am.command-opts"):
                    input = int(int(
                        map_df.loc[map_df['name'] == val, 'value'].iloc[0].split("-Xmx")[1])/1024/1024)
                else:
                    input = map_df.loc[map_df['name'] == val, 'value'].iloc[0]
                try:
                    sheet[cell] = float(input)
                except Exception:
                    continue

            xfile.save(f'yarn-tuning-guide-{cores}.xlsx')
