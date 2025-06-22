# Copyright 2025 TIER IV, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from caret_analyze.infra import RecordsProvider
from caret_analyze.record.column import ColumnValue
from caret_analyze.record.record_cpp_impl import RecordCppImpl, RecordsCppImpl
from caret_analyze.runtime.communication import Communication
from caret_analyze.value_objects import CommunicationStructValue
from caret_analyze.runtime.node import Node
from caret_analyze.runtime.publisher import Publisher
from caret_analyze.runtime.subscription import Subscription

class TestCommunication:

    def test_communication(self, mocker):
        records_provider = mocker.Mock(spec=RecordsProvider)
        pub_node = mocker.Mock(spec=Node)
        sub_node = mocker.Mock(spec=Node)
        publisher = mocker.Mock(sepc=Publisher)
        subscription = mocker.Mock(sepc=Subscription)
        communication_value = mocker.Mock(spec=CommunicationStructValue)
        communication = Communication(
            pub_node,
            sub_node,
            publisher,
            subscription,
            communication_value,
            records_provider,
            callbacks_publish = None,
            callback_subscription = None,
        )
        topic_name = 'topic0'
        callback_name = 'callback0'

        mocker.patch.object(
            records_provider, 'communication_records',
            return_value=RecordsCppImpl(
                [
                    RecordCppImpl({
                        f'{topic_name}/rclcpp_publish_timestamp': 0,
                        f'{topic_name}/rcl_publish_timestamp': 1,
                        f'{topic_name}/dds_publish_timestamp': 2,
                        f'{topic_name}/source_timestamp': 3,
                        f'{callback_name}/callback_start_timestamp': 4,
                    }),
                ],
                [
                    ColumnValue(f'{topic_name}/rclcpp_publish_timestamp'),
                    ColumnValue(f'{topic_name}/rcl_publish_timestamp'),
                    ColumnValue(f'{topic_name}/dds_publish_timestamp'),
                    ColumnValue(f'{topic_name}/source_timestamp'),
                    ColumnValue(f'{callback_name}/callback_start_timestamp'),
                ]
            )
        )
        records = communication.to_records()
        expected = RecordsCppImpl(
            [
                RecordCppImpl({
                    f'{topic_name}/rclcpp_publish_timestamp': 0,
                    f'{topic_name}/rcl_publish_timestamp': 1,
                    f'{topic_name}/dds_publish_timestamp': 2,
                    f'{topic_name}/source_timestamp': 3,
                    f'{callback_name}/callback_start_timestamp': 4,
                }),
            ],
            [
                ColumnValue(f'{topic_name}/rclcpp_publish_timestamp'),
                ColumnValue(f'{topic_name}/rcl_publish_timestamp'),
                ColumnValue(f'{topic_name}/dds_publish_timestamp'),
                ColumnValue(f'{topic_name}/source_timestamp'),
                ColumnValue(f'{callback_name}/callback_start_timestamp'),
            ]
        )

        assert records.equals(expected)
