import os
import shutil
import unittest
from unittest.mock import patch
import boto3
from moto import mock_aws
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, NestedField
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
from app import send_files_metrics, send_partition_metrics, send_snapshot_metrics

# Mock AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
os.environ['AWS_SECURITY_TOKEN'] = 'testing'
os.environ['AWS_SESSION_TOKEN'] = 'testing'

class TestIcebergMetrics(unittest.TestCase):

    @mock_aws
    @patch.dict(os.environ, {'CW_NAMESPACE': 'TestNamespace'})
    @patch('app.send_custom_metric')
    def setUp(self, mock_send_custom_metric):
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=os.environ.get('GLUE_REGION'))
        self.schema = Schema(
            NestedField(1, 'id', LongType(), False),
            NestedField(2, 'data', StringType(), False)
        )
        self.partition_spec = PartitionSpec(
            fields=[
                PartitionField(source_id=2, field_id=1000, name="data", transform="identity")
            ]
        )
    
        catalog_path = './tests/test_db'
        if os.path.exists(catalog_path):
            shutil.rmtree(catalog_path)
        os.makedirs(catalog_path)
        warehouse_path = os.path.abspath(catalog_path)
        self.catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
            },
        )
        self.catalog.create_namespace('default')
        self.catalog.create_table(
            'default.test_table',
            schema=self.schema,
            partition_spec=self.partition_spec
        )
        
        # Load the table and insert some data
        self.table = self.catalog.load_table(('default', 'test_table'))
        self.update_table(0, 5)

    def create_arrow_table(self, range_start, range_end):
        data = {
            'id': pa.array(range(range_start, range_end), pa.int64()),
            'data': pa.array(['data' + str(i) for i in range(range_start, range_end)], pa.string())
        }
        return pa.Table.from_pydict(data)
    
    @patch('app.send_custom_metric')
    def test_send_files_metrics(self, mock_send_custom_metric):
        result = send_files_metrics(self.table, self.snapshot)
        mock_send_custom_metric.assert_called()
        self.assertIn('avg_record_count', result)
        self.assertIn('max_record_count', result)
        self.assertIn('avg_file_size', result)
        self.assertIn('max_file_size', result)
    
    @patch('app.send_custom_metric')
    def test_send_partition_metrics(self, mock_send_custom_metric):
        result = send_partition_metrics(self.table, self.snapshot)
        mock_send_custom_metric.assert_called()
        self.assertIsNotNone(result)
    
    @patch('app.send_custom_metric')
    def test_send_snapshot_metrics(self, mock_send_custom_metric):
        result = send_snapshot_metrics(self.table, self.snapshot)
        mock_send_custom_metric.assert_called()
        self.assertIsNotNone(result)
    
    def update_table(self, range_start, range_end):
        # Perform an update operation on the Iceberg table
        arrow_table = self.create_arrow_table(range_start, range_end)
        self.table.append(arrow_table)
        self.table.refresh()
        self.snapshot = self.table.current_snapshot()
    
    @patch('app.send_custom_metric')
    def test_metrics_after_update(self, mock_send_custom_metric):
        self.update_table(5, 10)
        
        result_files_metrics = send_files_metrics(self.table, self.snapshot)
        mock_send_custom_metric.assert_called()
        self.assertIn('avg_record_count', result_files_metrics)
        self.assertIn('max_record_count', result_files_metrics)
        self.assertIn('avg_file_size', result_files_metrics)
        self.assertIn('max_file_size', result_files_metrics)
        
        result_partition_metrics = send_partition_metrics(self.table, self.snapshot)
        mock_send_custom_metric.assert_called()
        self.assertIsNotNone(result_partition_metrics)
        
        result_snapshot_metrics = send_snapshot_metrics(self.table, self.snapshot)
        mock_send_custom_metric.assert_called()
        self.assertIsNotNone(result_snapshot_metrics)

if __name__ == '__main__':
    unittest.main()
