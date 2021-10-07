import requests
import pandas as pd
from flask import Flask,request
import json
import urllib
from flask import jsonify
import ssl

app = Flask(__name__ , template_folder='templates')
context = ssl.SSLContext()
context.load_cert_chain('netauth.vmware.com-pem.cer', 'netauth_vmware_com.key')


def app_vm_details():
    data = {
      'refresh_token': 'QLU8xYZBGvpvlIgdDigSvUbKg4NtWL2GhwavKgkaUYVi5W3QYox5FvS4yI7mAJY8'
    }

    response = requests.post('https://console.cloud.vmware.com/csp/gateway/am/api/auth/api-tokens/authorize', data=data)
    access_token = response.json()['access_token']


    url_cmd= '''https://clearsky.svc.eng.vmware.com/api/v1/entity/application?fields=all&filter=%2383%3A2%20in%20out
    (%27ApplicationToBusinessUnit%27).%40rid&get_edge_prop=false&get_edges=true&ignore_pagination=false&is_active=true&
    linked_rec_props_regex=.*&page=1&per_page=50&sort_field=name'''

    response1 = requests.get(url_cmd, headers = {
        'Authorization' : 'Bearer ' + access_token})

    list_data =response1.json()['data']

    out = [i for i in list_data if i['entity_type'] == 'Application']

    application_list = []
    for j in out:

        # ****Application details extract*******
        properties = j['properties']
        application_name = properties['name']
        orid = properties['name']
        env_status = properties['status']
        outedges_details = j['out_edges']
        contact_list = [k['properties']['mail'] for k in outedges_details if k['entity_type'] == 'AdPerson']

        # ****VM details extract*******'
        vm_list = [k['properties']
                   for k in outedges_details if k['entity_type'] == 'VsphereVm']

        vm_details_list = []
        for k in vm_list:
            name = k['name']
            hostname = k['guest_hostname'] if 'guest_hostname' in list(k.keys()) else 'NA'
            # hostname = k['config_guest_os']
            orid = k['orid']
            ip_address = k['ip_address'] if 'ip_address' in list(k.keys()) else 'NA'
            powered_on = k['powered_on']
            num_cpu = k['num_cpu']
            num_cores_per_socket = k['num_cores_per_socket']
            disk_size_kb = k['disk_size_kb']
            memory_mb = k['memory_mb']
            os = k['guest_guest_os']
            esx_server_id = k['esx_server_id']
            vcenter_id = k['vcenter_id']
            vm_details_list.append(
                {'name': name, 'hostname': hostname, 'orid': orid, 'ip_address': ip_address, 'powered_on': powered_on,
                 'num_cpu': num_cpu, 'num_cores_per_socket': num_cores_per_socket, 'disk_size_kb': disk_size_kb,
                 'memory_mb': memory_mb, 'os': os, 'esx_server_id': esx_server_id, 'vcenter_id': vcenter_id})

        application_list.append({'name': application_name, 'orid': orid, 'env_status': env_status
                                    , 'contact_list': contact_list, 'vm_details': vm_details_list})

    return application_list

def esxi_vm_count(select_val):
    application_list = app_vm_details()
    df = pd.DataFrame(application_list)
    if select_val == 'all':

        df = df
    else:
        select_val = list(select_val.split(','))
        df['new_name'] = df['name'].str.lower()
        df = df[df['new_name'].isin(select_val)]

    #print(df)

    def split_col(x):

        esx_server_id = []
        for x_dict in x:
            esx_server_id.append(x_dict['esx_server_id'])

        return esx_server_id

    df['esx_server_id'] = df['vm_details'].apply(split_col)
    esx_server_list = df['esx_server_id'].tolist()
    esx_server_list = [j for l in esx_server_list for j in l]
    esx_id = list(set(esx_server_list))

    return esx_id

def app_vm_count(select_val):
    application_list = app_vm_details()
    df = pd.DataFrame(application_list)
    if select_val == 'all':
        print('all select')
        df = df
    else:
        select_val = list(select_val.split(','))
        print(select_val)
        df['new_name'] = df['name'].str.lower()
        df = df[df['new_name'].isin(select_val)]

    #print(df)
    def split_col(x):
        vm_name = []
        esx_server_id = []
        vcenter_id = []
        memory_mb = []
        disk_size_kb = []
        powered_on = []
        num_cpu = []
        os = []

        for x_dict in x:
            vm_name.append(x_dict['name'])
            esx_server_id.append(x_dict['esx_server_id'])
            vcenter_id.append(x_dict['vcenter_id'])
            memory_mb.append(x_dict['memory_mb'])
            disk_size_kb.append(x_dict['disk_size_kb'])
            powered_on.append(x_dict['powered_on'])
            num_cpu.append(x_dict['num_cpu'])
            os.append(x_dict['os'])

        return vm_name, esx_server_id, vcenter_id, memory_mb, disk_size_kb, powered_on, num_cpu, os

    df['vm_name'], df['esx_server_id'], df['vcenter_id'], df['memory_mb'], df['disk_size_kb'], df['powered_on'], df[
        'num_cpu'], df['os'] = zip(*df['vm_details'].map(split_col))

    from collections import Counter
    vm_name_list = df['vm_name'].tolist()
    vm_name_list = [j for l in vm_name_list for j in l]
    vm_count = len(set(vm_name_list))

    esx_server_list = df['esx_server_id'].tolist()
    esx_server_list = [j for l in esx_server_list for j in l]
    esx_id = set(esx_server_list)
    esxi_count = len(set(esx_server_list))

    vcenter_id_list = df['vcenter_id'].tolist()
    vcenter_id_list = [j for l in vcenter_id_list for j in l]
    vcenter_count = len(set(vcenter_id_list))

    memory_mb_list = df['memory_mb'].tolist()
    memory_mb_list = [int(j) for l in memory_mb_list for j in l]
    total_memory_vm = sum(memory_mb_list)

    cpu_count_list = df['num_cpu'].tolist()
    cpu_count_list = [int(j) for l in cpu_count_list for j in l]
    total_cpu_count_vm = sum(cpu_count_list)

    storage_list = df['disk_size_kb'].tolist()
    storage_list = [int(j) for l in storage_list for j in l]
    total_disk_storage_vm = sum(storage_list)

    power_status_list = df['powered_on'].tolist()
    power_status_list = [j for l in power_status_list for j in l]
    on_vm_count = sum(power_status_list)
    power_stat_vm = len(power_status_list)
    off_vm_count = power_stat_vm - on_vm_count

    os_platform_list = df['os'].tolist()
    os_platform_list = [j for l in os_platform_list for j in l]
    os_vm_count = dict(Counter(os_platform_list))

    env_status_list = df['env_status'].tolist()
    status_vm_count = dict(Counter(env_status_list))

    contact_list = ['Present' if len(l) > 0 else 'Absent' for l in df['contact_list']]
    contact_status_vm = dict(Counter(contact_list))

    result= {'application_count': len(df),
     'vm_count': vm_count, 'esxi_count': esxi_count, 'vcenter_count': vcenter_count,
             'total_memory_vm': total_memory_vm,
     'total_cpu_count_vm': total_cpu_count_vm, 'total_disk_storage_vm': total_disk_storage_vm,
     'off_vm_count': off_vm_count,
     'on_vm_count': on_vm_count, 'os_vm_count': os_vm_count, 'status_vm_count': status_vm_count,
     'contact_status_vm': contact_status_vm}
    return result

@app.route('/',  methods=['POST', 'GET'])
def index():

    return 'Application Check'

@app.route('/api/ipm/application_details', methods=['POST', 'GET'])
def application_details():
    application_detail_list = json.dumps(app_vm_details())

    return application_detail_list

@app.route('/api/ipm/aggregate_values', methods=['POST', 'GET'])
def aggregate_values():

    select_val = request.args.get("application").lower()
    result = app_vm_count(select_val)

    return jsonify(result)

@app.route('/api/ipm/esxi_list', methods=['POST', 'GET'])
def esxi_list():

    select_val = request.args.get("application").lower()
    x = esxi_vm_count(select_val)

    data = {
        'refresh_token': 'QLU8xYZBGvpvlIgdDigSvUbKg4NtWL2GhwavKgkaUYVi5W3QYox5FvS4yI7mAJY8'
    }

    response = requests.post('https://console.cloud.vmware.com/csp/gateway/am/api/auth/api-tokens/authorize', data=data)
    access_token = response.json()['access_token']
    list_data = []
    for j in range(0, len(x), 50):
        unspsc_links = [f' id in {x[j:j + 50]}']
        host_id = urllib.parse.quote(unspsc_links[0])

        url_cmd = 'https://clearsky.svc.eng.vmware.com/api/v1/entity/esxserver?fields=all&filter=' + host_id + ' &is_active=true&page=1&per_page=100&sort_field=name'
        response1 = requests.get(url_cmd, headers={
            'Authorization': 'Bearer ' + access_token})

        list_data.append(response1.json()['data'])

    esxi_properties = [j['properties'] for i in list_data for j in i]
    return json.dumps(esxi_properties)

@app.route('/api/ipm/catalog/esxi', methods=['POST', 'GET'])
def catalog_details():

    data = pd.read_excel('catalog_data.xlsx')
    result  = data.to_json(orient='records')

    return result

if __name__ == '__main__':
    app.run(port="5003", host="0.0.0.0", ssl_context=context)
