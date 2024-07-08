# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
from pathlib import Path

import boto3


class AmazonS3IO:
    r"""A class for accessing data in a Amazon S3 bucket. This class provides
    basic functionality for uploading, downloading, and deleting files. Before
    using this class, make sure that you have the necessary permissions to
    access the specified bucket and that you have configured your AWS
    credentials.

    Args:
        bucket_name (str): The name of the bucket.

    References:
        https://aws.amazon.com/pm/serv-s3/

        https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html

        https://docs.aws.amazon.com/IAM/latest/UserGuide/security-creds.html
        #sec-access-keys-and-secret-access-keys
    """

    def __init__(self, bucket_name: str) -> None:
        self._s3_resource = boto3.resource('s3')
        self._bucket_name = bucket_name
        self._bucket = self._s3_resource.Bucket(bucket_name)

    def upload_file(self, file_path: Path, file_content: str) -> None:
        r"""Uploads a file to the bucket.

        Args:
            file_path (Path): the path of the file in the bucket.
            file_content (str): The content of the file.
        """
        self._bucket.put_object(Key=str(file_path), Body=file_content)

    def download_file(self, file_path: Path) -> str:
        try:
            obj = self._bucket.Object(file_path)
            file_content = obj.get()['Body'].read().decode('utf-8')
            print(
                f'File {file_path} downloaded from bucket '
                f'{self._bucket_name}.'
            )
            return file_content
        except self._s3_resource.meta.client.exceptions.NoSuchKey:
            print(
                f'File {file_path} does not exist in bucket '
                f'{self._bucket_name}.'
            )
            return ""

    def delete_file(self, file_path: Path) -> None:
        self._bucket.Object(file_path).delete()

    def list_files(self) -> list[str]:
        files = [obj.key for obj in self._bucket.objects.all()]
        return files
