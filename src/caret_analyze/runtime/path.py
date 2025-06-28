# Copyright 2021 TIER IV, Inc.
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

from __future__ import annotations

from collections.abc import Sequence
from logging import getLogger

from .callback import CallbackBase
from .communication import Communication
from .node_path import NodePath
from .path_base import PathBase
from ..common import Summarizable, Summary, Util
from ..exceptions import Error, InvalidArgumentError, InvalidRecordsError
from ..record import (Columns,
                      merge, merge_sequential,
                      RecordsFactory,
                      RecordInterface, RecordsInterface)
from ..value_objects import CallbackChain, PathStructValue

logger = getLogger(__name__)


class ColumnMerger():

    def __init__(self) -> None:
        self._count: dict[str, int] = {}
        self._column_names: list[str] = []

    def append_column(
        self,
        records: RecordsInterface,
        column_name: str
    ) -> str:
        key = column_name

        if key == records.columns[0] and len(self._count) > 0 and key in self._count.keys():
            count = max(self._count.get(key, 0) - 1, 0)
            return self._to_column_name(count, key)

        if key not in self._count.keys():
            self._count[key] = 0

        column_name = self._to_column_name(self._count[key], key)
        self._column_names.append(column_name)
        self._count[key] += 1
        return column_name

    def append_columns(
        self,
        records: RecordsInterface,
    ) -> list[str]:
        renamed_columns: list[str] = []
        for column in records.columns:
            renamed_columns.append(
                self.append_column(records, column)
            )
        return renamed_columns

    def append_columns_and_return_rename_rule(
        self,
        records: RecordsInterface,
    ) -> dict[str, str]:
        renamed_columns: list[str] = self.append_columns(records)
        return self._to_rename_rule(records.columns, renamed_columns)

    @property
    def column_names(
        self
    ) -> list[str]:
        return self._column_names

    @staticmethod
    def _to_column_name(
        count: int,
        tracepoint_name: str,
    ) -> str:
        return f'{tracepoint_name}/{count}'

    @staticmethod
    def _to_rename_rule(
        old_columns: Sequence[str],
        new_columns: Sequence[str],
    ):
        return dict(zip(old_columns, new_columns))


class RecordsMerged:

    def __init__(
        self,
        merge_targets: list[NodePath | Communication],
        include_first_callback: bool = False,
        include_last_callback: bool = False
    ) -> None:
        if len(merge_targets) == 0:
            raise InvalidArgumentError('There are no records to be merged.')
        self._data = self._merge_records(
            merge_targets,
            include_first_callback,
            include_last_callback)

    @property
    def data(self) -> RecordsInterface:
        return self._data

    @staticmethod
    def _merge_records(
        targets: list[NodePath | Communication],
        include_first_callback: bool = False,
        include_last_callback: bool = False
    ) -> RecordsInterface:
        logger.info('Started merging path records.')

        def is_match_column(column: str, target_name: str) -> bool:
            last_slash_index = column.rfind('/')
            if last_slash_index >= 0:
                column = column[:last_slash_index]
            return column.endswith(target_name)

        print(f"DEBUG: [merge_records] 0. Initial targets list:")
        # pandasをインポート
        import pandas as pd
        pd.set_option('display.max_columns', None)

        # ここでtargetsがリストでない場合のTypeErrorを捕捉
        if not isinstance(targets, list):
            logger.error(f"Invalid 'targets' argument type: {type(targets)}. Expected list. Returning empty records.")
            return RecordsMerged(data_or_df=None, columns=[])

        for i, target_item in enumerate(targets):
            target_type = type(target_item).__name__
            target_name = target_item.node_name if hasattr(target_item, 'node_name') else \
                          target_item.subscribe_node_name if hasattr(target_item, 'subscribe_node_name') else "Unknown"
            print(f"DEBUG: [merge_records] 0.   Target {i}: Type={target_type}, Name={target_name}")
            initial_target_records = target_item.to_records()
            if len(initial_target_records.data) > 0:
                df_initial_records = initial_target_records.to_dataframe()
                print(f"DEBUG: [merge_records] 0.     Initial target {i} records:\n{df_initial_records}") # DEBUG
            else:
                print(f"DEBUG: [merge_records] 0.     Initial target {i} records data is empty, no values to display.")

        column_merger = ColumnMerger()
        
        # Add a check for empty targets list at the very beginning of the static method
        if not targets:
            logger.warning("No merge targets provided to _merge_records. Returning empty records.")
            return RecordsMerged(data_or_df=None, columns=[])

        temp_targets = list(targets)
        while temp_targets and len(temp_targets[0].to_records().data) == 0:
            logger.info(f"Skipping empty initial target: {temp_targets[0].node_name if hasattr(temp_targets[0], 'node_name') else 'Unknown'}")
            temp_targets = temp_targets[1:]
        
        if not temp_targets:
            logger.warning("All initial targets were empty. Returning empty records.")
            return RecordsFactory.create_instance()
        
        first_element = temp_targets[0].to_records()
        targets = temp_targets

        left_records: RecordsInterface = first_element 

        rename_rule = column_merger.append_columns_and_return_rename_rule(left_records)
        left_records.rename_columns(rename_rule)
        first_column = left_records.columns[0] if left_records.columns else None 

        last_communication_in_full_list = None
        for item in reversed(targets):
            if isinstance(item, Communication):
                last_communication_in_full_list = item
                break
        
        # =========================================================================
        # --- [修正箇所 1] to_take_records() から変換されたカラム名を保持する変数の導入 ---
        # =========================================================================
        converted_rmw_take_timestamp_column: str | None = None
        # =========================================================================

        print("!!! 1")
        remain_last: list[NodePath | Communication] = []
        remain_first: list[NodePath | Communication] = []

        if len(targets) > 1:
            for target_idx in range(len(targets) - 1):
                target_ = targets[target_idx]
                target = targets[target_idx + 1]

                remain_last = target_
                remain_first = target
                print("!!! 1-1")
                right_records: RecordsInterface = target.to_records()

                is_dummy_records = len(right_records.columns) == 0 or len(right_records.data) == 0

                if is_dummy_records:
                    if target == targets[-1]:
                        msg = 'Detected dummy_records. merge terminated.'
                        logger.info(msg)
                    else:
                        msg = 'Detected dummy_records before merging end_records. merge terminated.'
                        logger.warning(msg)
                    print("!!! 1-2")
                    break
                print("!!! 1-3")
                if isinstance(target, Communication) and target.use_take_manually():
                    print("!!! 1-5")
                    
                    if target == last_communication_in_full_list:
                        print("!!! 1-5-1")
                        print("!!! Applying right_records = target.to_take_records() for the identified last Communication target")
                        try:
                            right_records = target.to_take_records()
                            
                            # =========================================================================
                            # --- [修正箇所 2] to_take_records() 後のカラム名変換ロジック ---
                            # =========================================================================
                            rename_map = {}
                            for col in right_records.columns:
                                # 例: /planning/planning_validator/callback_6/callback_start_timestamp/0
                                # を /planning/planning_validator/rmw_take_timestamp/0 に変換
                                import re
                                match = re.match(r'(.*/)(callback_\d+/callback_start_timestamp)(/.*)', col)
                                if match:
                                    original_prefix = match.group(1)
                                    original_suffix = match.group(3)
                                    new_col_name = f"{original_prefix}rmw_take_timestamp{original_suffix}"
                                    rename_map[col] = new_col_name
                                    # 変換されたカラム名を保持（最終削除から除外するため）
                                    converted_rmw_take_timestamp_column = new_col_name
                                    logger.info(f"Renamed column for take_records: {col} -> {new_col_name}")
                                elif "rmw_take_timestamp" in col:
                                    # 元々rmw_take_timestampが含まれるカラムも念のため保持対象とする
                                    converted_rmw_take_timestamp_column = col
                                    logger.info(f"Identified existing rmw_take_timestamp column to preserve: {col}")


                            if rename_map:
                                right_records.rename_columns(rename_map)
                                logger.info(f"Applied rename for take_records. New columns: {right_records.columns}")
                            # =========================================================================

                        except Exception as e:
                            msg = f"Failed to get take records for the last Communication target: {e}"
                            print(msg)
                            logger.error(msg)
                            raise InvalidRecordsError(msg)
                    else:
                        print("!!! 1-5-2")
                        print("!!! 1-5-2 (Original drop_columns logic for other Communication targets)")
                        if right_records.columns:
                            right_records.drop_columns([right_records.columns[-1]])
                        
                print("!!! 1-4 (Calling append_columns_and_return_rename_rule once)")
                rename_rule = column_merger.append_columns_and_return_rename_rule(right_records)
                right_records.rename_columns(rename_rule)

                if not left_records.columns or not right_records.columns:
                    print(f"### 1-6 (Skipping column check due to empty columns. Left: {left_records.columns}, Right: {right_records.columns})")
                    raise InvalidRecordsError('Empty columns encountered during merge. Cannot proceed.')

                print(f"\n--- {left_records.columns[-1]=}, {right_records.columns[0]=}")
                # ここでの比較は、リネーム後のright_records.columns[0]を使用
                if left_records.columns[-1] != right_records.columns[0]:
                    print("### 1-6")
                    print(f'### !!! left columns[-1]: {left_records.columns[-1]} != right columns[0]: {right_records.columns[0]}')
                    print("### !!! {left_records.to_dataframe()=}")
                    print("### !!! {right_records.to_dataframe()=}")
                    raise InvalidRecordsError('left columns[-1] != right columns[0]')

                print("!!! 1-7")
                left_stamp_key = left_records.columns[-1]
                right_stamp_key = right_records.columns[0]

                if left_records.columns:
                    if left_records.columns[0] in right_records.columns:
                        right_records.drop_columns([left_records.columns[0]])
                    
                right_stamp_key = right_records.columns[0] if right_records.columns else None

                print("!!! 1-8")
                logger.info(
                    '\n[merge_sequential] \n'
                    f'- left_column: {left_stamp_key} \n'
                    f'- right_column: {right_stamp_key} \n'
                )

                is_sequential = isinstance(target_, NodePath) and \
                                isinstance(target, Communication) and \
                                isinstance(target_.message_context, CallbackChain)

                if right_stamp_key is None:
                    logger.warning("Right stamp key is None, skipping merge for current iteration.")
                    continue

                if is_sequential:
                    print("!!! 1-9")
                    left_records = merge_sequential(
                        left_records=left_records,
                        right_records=right_records,
                        join_left_key=None,
                        join_right_key=None,
                        left_stamp_key=left_stamp_key,
                        right_stamp_key=right_stamp_key,
                        columns=Columns.from_str(
                            left_records.columns + right_records.columns
                        ).column_names,
                        how='left_use_latest',
                    )
                    print("!!! 1-10")
                else:
                    print("!!! 1-11")
                    left_records = merge(
                        left_records=left_records,
                        right_records=right_records,
                        join_left_key=left_records.columns[-1],
                        join_right_key=right_records.columns[0],
                        columns=Columns.from_str(
                            left_records.columns + right_records.columns
                        ).column_names,
                        how='left'
                    )
                    print("!!! 1-12")
        else:
            logger.info("Only one target provided, no merging loop executed.")


        print("!!! 2")
        if targets:
            print(f"DEBUG: [merge_records] -- {type(remain_last).__name__}: {remain_last.to_records().to_dataframe() if remain_last else 'N/A'}= (remain_last)")
            print(f"DEBUG: [merge_records] -- {type(remain_first).__name__}: {remain_first.to_records().to_dataframe() if remain_first else 'N/A'}= (remain_first)")
        else:
            print("DEBUG: [merge_records] No targets to display remain_last/first.")

        if include_last_callback and targets and isinstance(targets[-1], NodePath):
            
            right_records = targets[-1].to_path_end_records()

            print("!!! 2-1")
            rename_rule = column_merger.append_columns_and_return_rename_rule(right_records)
            right_records.rename_columns(rename_rule)
            
            if left_records.columns and right_records.columns and \
               left_records.columns[-1] == right_records.columns[0]:
                print("!!! 2-3")
                if len(right_records.data) != 0:
                    print("!!! 2-4")
                    left_records = merge(
                        left_records=left_records,
                        right_records=right_records,
                        join_left_key=left_records.columns[-1],
                        join_right_key=right_records.columns[0],
                        columns=Columns.from_str(
                            left_records.columns + right_records.columns
                        ).column_names,
                        how='left'
                    )
                    print("!!! 2-5")
                else:
                    print("!!! 2-6")
                    msg = 'Empty records are not merged for last callback.'
                    logger.warning(msg)
            else:
                print("### 2-2")
                print(f'### !!! Last callback merge skipped due to key mismatch or empty columns. '
                      f'Left key: {left_records.columns[-1] if left_records.columns else "N/A"}, '
                      f'Right key: {right_records.columns[0] if right_records.columns else "N/A"}')
                msg = 'Since the path cannot be extended (key mismatch or empty records), '
                msg += 'the merge process for the last callback record is skipped.'
                logger.warning(msg)
            print("!!! 2-7")
        else:
            print("!!! 2-8")
            if include_last_callback:
                logger.warning("include_last_callback is True, but the last target is not a NodePath or targets list is empty. Skipping last callback merge.")
            else:
                logger.info("include_last_callback is False. Skipping last callback merge.")
        
        print("!!! 2-9")
        logger.info('Finished merging path records.')
        
        if first_column:
            left_records.sort(first_column)

        print("!!! 3")
        source_columns = [
            column for column in left_records.columns
            if is_match_column(column, 'source_timestamp')
        ]
        left_records.drop_columns(source_columns)

        print("!!! 4")
        rmw_take_column = [
            column for column in left_records.columns
            if is_match_column(column, 'rmw_take_timestamp')
        ]

        # =========================================================================
        # --- [修正箇所 3] to_take_records() から変換されたカラム名を最終削除から除外 ---
        # =========================================================================
        if converted_rmw_take_timestamp_column and converted_rmw_take_timestamp_column in rmw_take_column:
            rmw_take_column.remove(converted_rmw_take_timestamp_column)
            logger.info(f"Removed '{converted_rmw_take_timestamp_column}' from rmw_take_column list before final drop as it was converted.")
        else:
            logger.info("No specific RMW take column was converted and needed to be preserved from the final drop, or it's not in the list to be dropped.")
        # =========================================================================

        left_records.drop_columns(rmw_take_column)

       
        ##################################
### **行レベルフィルタリング (0 または NA 値)**

        if left_records.data:
            logger.info("Applying row-level filtering to remove records with 0 or NA in any applicable column.")
            
            # フィルタリング対象の部分文字列を定義
            # このリストに含まれる文字列がカラム名の一部として存在する場合にチェックします。
            TARGET_SUBSTRINGS = [
                'rclcpp_publish_timestamp',
                'callback_start_timestamp',
                'rmw_take_timestamp'
            ]

            def filter_func(record: RecordInterface) -> bool:
                # このレコードが実際に持っている全てのリカラム名を取得します。
                # RecordInterface.columns は set[str] を返します。
                for col_name in record.columns: 
                    # カラム名がTARGET_SUBSTRINGSのいずれかを含むかチェックします。
                    is_relevant_column = any(
                        substring in col_name for substring in TARGET_SUBSTRINGS
                    )

                    if not is_relevant_column:
                        continue # 関係ないカラムであればスキップします。

                    # record.get() メソッドを使用して値を取得します。
                    # RecordInterfaceの定義に基づき、get()はintを返すとされています。
                    # しかし、もし値が存在しない場合にget()が0を返すのであれば、
                    # Noneチェックは不要になる可能性がありますが、安全のため残しておきます。
                    value = record.get(col_name) 
                    
                    # 値が0であれば、そのレコードは無効と判断し、削除対象とします。
                    # `value is None` のチェックは、get()がNoneを返さない前提であれば、
                    # ここでは主に論理的な完全性を保つために残します。
                    if value == 0 or value is None: 
                        logger.debug(f"Filtering out record due to {col_name}={value} (column: {col_name}, record: {record.data})")
                        return False # False を返すとそのレコードは削除されます。
                return True # すべてのチェックを通過したレコードは保持されます。

            left_records.filter_if(filter_func)
            
            initial_row_count = len(first_element.data) if 'first_element' in locals() else 0
            if len(left_records.data) < initial_row_count:
                 logger.info(f"Removed {initial_row_count - len(left_records.data)} rows due to 0 or NA values in applicable columns.")
            else:
                 logger.info("No rows removed based on 0 or NA values in applicable columns.")

        else:
            logger.info("No data in left_records to apply row-level filtering.")

        ### **列レベルフィルタリング (すべて 0 またはすべて NA)**

        columns_to_drop_by_content = []
        if left_records.data:
            try:
                # to_dataframe() は RecordsInterface のメソッドとして存在し、
                # これは内部データに基づいてDataFrameを生成すると想定されます。
                df_temp = left_records.to_dataframe()

                for col in df_temp.columns:
                    if df_temp[col].isna().all():
                        columns_to_drop_by_content.append(col)
                    elif pd.api.types.is_numeric_dtype(df_temp[col]) and (df_temp[col] == 0).all():
                        columns_to_drop_by_content.append(col)
                    
                if columns_to_drop_by_content:
                    unique_cols_to_drop = list(set(columns_to_drop_by_content))
                    logger.info(f"Applying column-level filtering: Dropping columns: {unique_cols_to_drop}")
                    left_records.drop_columns(unique_cols_to_drop)
                else:
                    logger.info("No columns identified for dropping based on all 0 or all NA content.")

            except Exception as e:
                logger.error(f"Error during column-level filtering: {e}")
        else:
            logger.info("No data in left_records to apply column-level filtering.")

        # --- フィルタリングロジックここまで ---

        print(f"--- Final left_records: {left_records.to_dataframe()} ---")

        return left_records


class Path(PathBase, Summarizable):
    """
    A class that represents a path.

    A single path is composed of node paths and communications.
    """

    def __init__(
        self,
        path: PathStructValue,
        child: list[NodePath | Communication],
        callbacks: list[CallbackBase] | None,
        include_first_callback: bool = False,
        include_last_callback: bool = False
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        path : PathStructValue
            static info
        child : list[NodePath | Communication]
            path children's which compose path (node paths and communications).
        callbacks : list[CallbackBase] | None
            callbacks that compose the path.
            return None except for all of node paths are not callback-chain.
        include_first_callback : bool
            Flags for including the processing time of the first callback in the path analysis.
        include_last_callback : bool
            Flags for including the processing time of the last callback in the path analysis.

        """
        super().__init__()

        self._value = path
        self._validate(child)
        self._child = child
        self._columns_cache: list[str] | None = None
        self._callbacks = callbacks
        self._include_first_callback = include_first_callback
        self._include_last_callback = include_last_callback
        self.__records_cache: dict = {}
        return None

    @property
    def include_first_callback(self) -> bool:
        return self._include_first_callback

    @include_first_callback.setter
    def include_first_callback(self, include_first_callback: bool) -> None:
        self._include_first_callback = include_first_callback

    @property
    def include_last_callback(self) -> bool:
        return self._include_last_callback

    @include_last_callback.setter
    def include_last_callback(self, include_last_callback: bool) -> None:
        self._include_last_callback = include_last_callback

    def to_records(self) -> RecordsInterface:
        if (self._include_first_callback, self._include_last_callback) \
                not in self.__records_cache.keys():
            try:
                self.__records_cache[(self._include_first_callback, self._include_last_callback)] \
                    = self._to_records_core()
            except Error as e:
                logger.warning(e)
                self.__records_cache[(self._include_first_callback, self._include_last_callback)] \
                    = RecordsFactory.create_instance()

        assert (self._include_first_callback, self._include_last_callback) \
            in self.__records_cache.keys()
        return self.__records_cache[(self._include_first_callback,
                                     self._include_last_callback)].clone()

    def _to_records_core(self) -> RecordsInterface:
        self._verify_path(self.node_paths)
        return RecordsMerged(self.child,
                             self._include_first_callback, self._include_last_callback).data

    @staticmethod
    def _verify_path(
        path_children: list[NodePath]
    ) -> None:
        for child in path_children[1: -1]:
            if len(child.column_names) == 0:
                msg = 'Node latency is empty. To see more details, execute [ path.verify() ].'
                logger.warning(msg)

    def verify(self) -> bool:
        """
        Verify whether the path can generate latencies.

        Returns
        -------
        bool
            True if both architecture and measurement results are valid, otherwise false.

        """
        is_valid = True
        for child in self.node_paths[1:-1]:
            if child.message_context is not None:
                continue
            msg = 'Detected invalid message context. Correct these node_path definitions. \n'
            msg += 'To see node definition,'
            msg += f"execute [ app.get_node('{child.node_name}').summary.pprint() ] \n"
            msg += str(child.summary)
            logger.warning(msg)
            is_valid = False

        for comm in self.communications:
            is_valid &= comm.verify()
        return is_valid

    def get_child(self, name: str):
        # TODO(hsgwa): This function is not needed. Remove.

        if not isinstance(name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(child: NodePath | Communication):
            if isinstance(child, NodePath):
                return child.node_name == name
            elif isinstance(child, Communication):
                return child.topic_name == name

        return Util.find_one(is_target, self.child)

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            summary info.

        """
        return self._value.summary

    @property
    def callbacks(self) -> list[CallbackBase]:
        """
        Get callbacks.

        Returns
        -------
        list[CallbackBase]
            callbacks in all nodes that comprise the node path.

        """
        callbacks = Util.flatten(
            comm.publish_node.callbacks for comm in self.communications
            if comm.publish_node.callbacks
        )
        if self.communications[-1].subscribe_node.callbacks is not None:
            callbacks.extend(self.communications[-1].subscribe_node.callbacks)

        return callbacks

    @property
    def callback_chain(self) -> list[CallbackBase] | None:
        """
        Get callback chain.

        Returns
        -------
        list[CallbackBase] | None
            callbacks that compose the path.
            return None except for all of the node paths are callback chains.

        """
        return self._callbacks

    @staticmethod
    def _validate(path_elements: list[NodePath | Communication]):
        if len(path_elements) == 0:
            return
        t = NodePath if isinstance(
            path_elements[0], NodePath) else Communication
        for e in path_elements[1:]:
            if t == Communication:
                t = NodePath
            else:
                t = Communication
            if isinstance(e, t):
                continue
            msg = 'NodePath and Communication should be alternated.'
            raise InvalidArgumentError(msg)

    @property
    def path_name(self) -> str | None:
        """
        Get path name.

        Returns
        -------
        str | None
            Path name defined in the architecture.

        """
        return self._value.path_name

    @property
    def value(self) -> PathStructValue:
        """
        Get StructValue object.

        Returns
        -------
        PathStructValue
            path value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._value

    def clear_cache(self) -> None:
        """Clear to_records/to_dataframe cache."""
        self._columns_cache = None
        self.__records_cache = {}
        return super().clear_cache()

    def __str__(self) -> str:
        node_names = [n.node_name for n in self.node_paths]
        return '\n'.join(node_names)

    @property
    def communications(self) -> list[Communication]:
        """
        Get communications.

        Returns
        -------
        list[Communication]
            Communications in target path.

        """
        return Util.filter_items(lambda x: isinstance(x, Communication), self._child)

    @property
    def node_paths(self) -> list[NodePath]:
        """
        Get node-paths.

        Returns
        -------
        list[NodePath]
            node paths in target path.

        """
        return Util.filter_items(lambda x: isinstance(x, NodePath), self._child)

    @property
    def topic_names(self) -> list[str]:
        """
        Get topic names.

        Returns
        -------
        list[str]
            topic names in the target path.

        """
        return sorted(self._value.topic_names)

    @property
    def child(self) -> list[NodePath | Communication]:
        """
        Get path children.

        Returns
        -------
        list[NodePath | Communication]
            node paths and communications in the target path.
            node paths and communications are alternately contained.

        """
        return self._child

    @property
    def child_names(self) -> list[str]:
        """
        Get path children's names.

        Returns
        -------
        list[str]
            node names and topic names in the target path.

        """
        return sorted(self._value.child_names)

    @property
    def node_names(self) -> list[str]:
        """
        Get node names.

        Returns
        -------
        list[str]
            node names in the target path.

        """
        return sorted(self._value.node_names)
