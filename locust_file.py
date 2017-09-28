from locust import HttpLocust, TaskSet, task
from requests.auth import HTTPBasicAuth
from requests import Session
from faker import Faker
import random
import six
import tempfile
import os
import pprint

BYTES_IN_MB = 1048576
MAX_CHUNK_SIZE = BYTES_IN_MB * 64
REQ_BUFFER_SIZE = 65536

import loggra
loggra.setup_graphite_communication()

class MyTaskSet(TaskSet):

    upload_file_paths = [
        ('data/100mb.bin', 100 * BYTES_IN_MB),
        ('data/10mb.bin', 10 * BYTES_IN_MB),
        ('data/1mb.bin', 1 * BYTES_IN_MB)
    ]

    def on_start(self):
        self.faker = Faker()
        self.create_user()
        self.login()

    def create_user(self):
        self.admin_session = Session()
        r = self.admin_session.get(self.locust.host + "/api/v1/user/authentication",
                               auth=HTTPBasicAuth('girder', 'girder'))
        r.raise_for_status()

        self.admin_session.headers.update({
            'Girder-Token': r.json()['authToken']['token']
        })

        # create a local fake profile
        self.user_profile = self.faker.profile()
        # set the local fake profiles username
        self.user_profile['password'] = 'letmein'

        # Use the admin user to create the girder user with the local fake profile info
        r = self.admin_session.post(self.locust.host + "/api/v1/user", {
            "login": self.user_profile['username'],
            "email": self.user_profile['mail'],
            "firstName": self.user_profile['name'].split(" ")[0],
            "lastName": self.user_profile['name'].split(" ")[1],
            "password": self.user_profile['password'],
            "admin": False
        })

        # Set the user_id locally
        self.user_id = r.json()['_id']

    def login(self):
        # Login as the user
        r = self.client.get("/api/v1/user/authentication",
                            auth=HTTPBasicAuth(self.user_profile['username'],
                                               self.user_profile['password']))
        r.raise_for_status()

        self.client.headers.update({
            'Girder-Token': r.json()['authToken']['token']
        })

        # Start a dict of folders (Public & Private) for this user
        # self.folders is a dict in the form folder_id => depth
        # this lets us manage the folder depth probabilistically
        r = self.client.get("/api/v1/folder",
                            name="misc",
                            params={'parentType': 'user',
                                    'parentId': self.user_id})
        r.raise_for_status()
        self.folders = {f["_id"]:1 for f in r.json()}
        self.files = []

    def _select_parent_folder(self, depth=1, decay=0.8):
        """ Select a parent folder in which to create a new folder, item or file.

        Prefer folders higher up in the tree. E.g. if decay is 0.8 then 80% of the time
        it will choose to create a folder at level 2 of the tree. 20% of the time it will
        recurse in which case 80% of the 20% of the time it will select a folder at level 3
        and 20% of the original 20% of the time it will recurse again - and so on and so on.

        Returns the id of the parent folder,  and the depth of that folder.
        """
        if depth == max(self.folders.values()) or random.random() < decay:
            return random.choice([i for i,d in self.folders.items() if d == depth ]), depth
        else:
            return self._select_parent_folder(depth=depth+1, decay=decay)

    @task(10)
    def create_folder(self):
        parent_id, depth = self._select_parent_folder()

        folder_name = self.faker.slug()

        # Ensure slug is unique for this user
        # This is slightly over safe seeing as names only need
        # to be unique with-in each folder,  not globally per-user
        while folder_name in self.folders:
            folder_name = self.faker.slug()

        # create folder
        r = self.client.post("/api/v1/folder",
                             name='api.v1.folder.{}.{}'.format(parent_id, folder_name),
                             params={'parentId': parent_id,
                                     'name': folder_name})
        r.raise_for_status()

        self.folders[r.json()['_id']] = depth + 1

    @task(45)
    def upload_file(self):
        # decay = 0.1 means prefer leaf folders
        folder_id, _ = self._select_parent_folder(decay=0.1)

        path, size = random.choice(self.upload_file_paths)
        offset = 0
        slug = self.faker.slug()

        r = self.client.post('/api/v1/file',
                             # name='/api/v1/file, %s, %s, %s, %s' % (size, offset, folder_id, slug))
                             name='api.v1.folder.{}'.format(size),
                             params={
                                 'parentType': 'folder',
                                 'parentId': folder_id,
                                 'name': slug,
                                 'size': size,
                                 'mimeType': 'application/octet-stream'
                             })
        uploadObj = r.json()

        if '_id' not in uploadObj:
            raise Exception(
                'After uploading a file chunk, did not receive object with _id. '
                'Got instead: ' + json.dumps(uploadObj))


        with open(path, 'rb') as stream:
            while True:
                chunk = stream.read(min(MAX_CHUNK_SIZE, (size - offset)))

                if not chunk:
                    break

                if isinstance(chunk, six.text_type):
                    chunk = chunk.encode('utf8')

                r = self.client.post('/api/v1/file/chunk',
                                     name='api.v1.file.chunk.{}.{}'.format(uploadObj['_id'], size),
                                     params={'offset': offset, 'uploadId': uploadObj['_id']},
                                     data=chunk)
                uploadObj = r.json()

                if '_id' not in uploadObj:
                    raise Exception(
                        'After uploading a file chunk, did not receive object with _id. '
                        'Got instead: ' + json.dumps(uploadObj))

                offset += len(chunk)

        self.files.append((uploadObj['_id'], size))

    @task(45)
    def download_file(self):
        if len(self.files) is 0:
            self.upload_file()

        file_id , size = random.choice(self.files)

        r = self.client.get('/api/v1/file/%s/download' % file_id,
                            name='api.v1.file.{}.download.{}'.format(file_id, size),
                            stream=True)

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            for chunk in r.iter_content(chunk_size=REQ_BUFFER_SIZE):
                tmp.write(chunk)

            os.remove(tmp.name)



class MyLocust(HttpLocust):
    task_set = MyTaskSet
    min_wait = 3000
    max_wait = 7000
