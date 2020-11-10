import time
import json
import random

import boto3
import uuid

from datetime import datetime
from faker import Faker


class KinesisLoader:
    def __init__(self, batch_size=500, maximum_records=None):
        """
        The default batch_size here is to match the maximum allowed by Kinesis in a PutRecords request
        """
        self.batch_size = min(batch_size, 500)
        self.maximum_records = maximum_records
        self.kinesis_client = boto3.session.Session().client('kinesis')
        self.faker = Faker()

    def generate_and_submit(self):
        counter = 0
        # Simple cutoff here - guaranteed to not send in more than maximum_records, with single batch granularity
        while counter < self.maximum_records and counter < self.maximum_records - self.batch_size:
            records_batch = [self.get_kinesis_record() for _ in range(0, self.batch_size)]
            request = {
                'Records': records_batch,
                'StreamName': 'user_action_stream'
            }

            response = self.kinesis_client.put_records(**request)
            self.submit_batch_until_successful(records_batch, response)

            counter += len(records_batch)
            print('Batch inserted. Total records: {}'.format(str(counter)))

        return

    def submit_batch_until_successful(self, batch, response):
        """ If needed, retry a batch of records, backing off exponentially until it goes through"""
        retry_interval = 0.5

        failed_record_count = response['FailedRecordCount']
        while failed_record_count:
            time.sleep(retry_interval)

            # Failed records don't contain the original contents - we have to correlate with the input by position
            failed_records = [batch[i] for i, record in enumerate(response['Records']) if 'ErrorCode' in record]

            print('Incrementing exponential back off and retrying {} failed records'.format(str(len(failed_records))))
            retry_interval = min(retry_interval * 2, 10)
            request = {
                'Records': failed_records,
                'StreamName': 'user_action_stream'
            }

            result = self.kinesis_client.put_records(**request)
            failed_record_count = result['FailedRecordCount']

    def get_kinesis_record(self,):
        """
        Generate an item with a random hash key on a large range, and a unique sort key, and  a created date
        """
        item = {'uuid': str(uuid.uuid4()),
                'name': self.faker.name(),
                'job': self.faker.job(),
                'phone': self.faker.phone_number(),
                'email': self.faker.email(),
                'behavior': random.choice(['buy','sell']),
                'count': self.faker.random_int(1, 100),
                'datetime': datetime.utcnow().isoformat()}
        raw_data = json.dumps(item)
        return {
            'Data': bytes(raw_data),
            'PartitionKey': item['uuid'],
        }


if __name__ == "__main__":
    # execute only if run as a script
    loader = KinesisLoader(batch_size=50, maximum_records=1000)
    loader.generate_and_submit()
