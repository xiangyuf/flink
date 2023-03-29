################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import os
import version
import requests
from typing import Optional


template = '''################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
The pyflink version will be consistent with the flink version and follow the PEP440.
.. seealso:: https://www.python.org/dev/peps/pep-0440
"""
__version__ = "{}"
'''

def segments(s: str):
    s_split = s.replace(' ', '').split('.')
    return len(s_split)

def compare_version(v1: str, v2: str):
    '''
    Compare two version number.
    return positive if v1 greater than v2,
    return negative if v1 less than v2,
    return 0 if v1 equals to v2.
    For example, compare_version('1.16.1', '1.16.0') return positive number.
    '''
    v1_split = v1.replace(' ', '').split('.')
    v2_split = v2.replace(' ', '').split('.')
    if len(v1_split) != len(v2_split):
        raise RuntimeError('Can not compare two versions with different segments between {} and {}'.format(v1, v2))
    for i in range(len(v1_split)):
        if int(v1_split[i]) != int(v2_split[i]):
            return int(v1_split[i]) - int(v2_split[i])
    return 0

def version_plus_one(v: str):
    v_split = v.replace(' ', '').split('.')
    v_split[-1] = str(int(v_split[-1])+1)
    return '.'.join(v_split)

def get_new_version() -> Optional[str]:
    '''
    get the latest version of byted-apache-flink and byted-apache-flink-libraries from internal PyPI repository
    , then choose the larger one, finally use ${latest_version}+1 as the new generated version.
    :return: new version used as the version number in this publish
    '''
    headers = {"Authorization": 'Basic Ym8ueXU6RXhWNHF2aFMycEwwOE8='}
    url_pyflink = 'https://scm.byted.org/api/v2/pip_versions/316/version_filter/?page_num=1&status=build_ok&branch=&page_size=100&limit=100&offset=0'
    url_pyflink_lib = 'https://scm.byted.org/api/v2/pip_versions/315/version_filter/?page_num=1&status=build_ok&branch=&page_size=100&limit=100&offset=0'
    latest_version_pyflink = None
    latest_version_pyflink_lib = None

    rsp = requests.get(url_pyflink, headers=headers)
    if rsp.ok:
        pyflink_packages = rsp.json().get('results', [])
        for package in pyflink_packages:
            if segments(package.get('version')) != 3:
                continue
            if latest_version_pyflink is None:
                latest_version_pyflink = package.get('version')
            if compare_version(package.get('version'), latest_version_pyflink) > 0:
                latest_version_pyflink = package.get('version')
    else:
        print(rsp.content)

    rsp = requests.get(url_pyflink_lib, headers=headers)
    if rsp.ok:
        pyflink_lib_packages = rsp.json().get('results', [])
        for package in pyflink_lib_packages:
            if segments(package.get('version')) != 3:
                continue
            if latest_version_pyflink_lib is None:
                latest_version_pyflink_lib = package.get('version')
            if compare_version(package.get('version'), latest_version_pyflink_lib) > 0:
                latest_version_pyflink_lib = package.get('version')
    else:
        print(rsp.content)

    if latest_version_pyflink is not None and \
        latest_version_pyflink_lib is not None and \
        segments(latest_version_pyflink) == segments(latest_version_pyflink_lib):
        if compare_version(latest_version_pyflink, latest_version_pyflink_lib) >= 0:
            return version_plus_one(latest_version_pyflink)
        else:
            return version_plus_one(latest_version_pyflink_lib)
    elif latest_version_pyflink is not None and latest_version_pyflink_lib is None:
        return version_plus_one(latest_version_pyflink)
    elif latest_version_pyflink is None and latest_version_pyflink_lib is not None:
        return version_plus_one(latest_version_pyflink_lib)
    else:
        return None

if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.abspath(__file__))
    new_version = get_new_version()
    if new_version is not None and len(new_version) != 0:
        print('set version of pyflink to: {}'.format(new_version))
        content = template.format(new_version)
        with open('{}/version.py'.format(dir_path), 'w') as writer:
            writer.write(content)
    else:
        print('using default pyflink version: {}'.format(version.__version__))
