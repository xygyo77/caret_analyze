# Copyright 2021 Research Institute of Systems Planning, Inc.
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
from calendar import c

from bokeh.models import GraphRenderer, Legend
from bokeh.plotting import Figure

from .util import (apply_x_axis_offset, ColorSelectorFactory,
                   HoverKeysFactory, init_figure)
from ....runtime import Path
from ....common import ClockConverter
from ....record import RecordsInterface
from ....record.record_factory import RecordFactory, RecordsFactory
from ....record.column import Columns, ColumnValue

from ....common import loc

class BokehStackedBar:

    def __init__(
        self,
        metrics,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str,
    ) -> None:
        self._metrics = metrics
        self._xaxis_type = xaxis_type
        self._ywheel_zoom = ywheel_zoom
        self._full_legends = full_legends
        self._case = case

    def create_figure(self) -> Figure:
        # NOTE: relation between stacked bar graph and data struct
        # # data = {
        # #     a : [a1, a2, a3],
        # #     b : [b1, b2, b3],
        # #     'start time': [s1, s2, s3]
        # # }
        # # y_labels = [a, b]

        # # ^               ^
        # # |               |       ^       [] a
        # # |       ^       |       |       [] b
        # # |       |       a2      |
        # # |       a1      ^       a3
        # # |       ^       |       ^
        # # |       |       |       |
        # # |       b1      b2      b3
        # # +-------s1------s2------s3---------->

        loc.loc()

        # # get stacked bar data
        data: dict[str, list[int | float]]
        y_labels: list[str] = []
        y_axis_label = 'latency [ms]'
        target_objects = self._metrics.target_objects
        data, y_labels = self._metrics.to_stacked_bar_data()
        title: str = f"Stacked bar of '{getattr(target_objects, 'path_name')}'"

        fig = init_figure(title, self._ywheel_zoom, self._xaxis_type, y_axis_label)
        frame_min = data['start time'][0]
        frame_max = data['start time'][-1]
        x_label = 'start time'
        converter: ClockConverter | None = None
        if self._xaxis_type == 'system_time':
            apply_x_axis_offset(fig, frame_min, frame_max)
        elif self._xaxis_type == 'index':
            x_label = 'index'
        else:  # sim_time
            assert isinstance(target_objects, Path)
            assert len(target_objects.child) > 0
            # TODO(hsgwa): refactor
            provider = target_objects.child[0]._provider  # type: ignore
            converter = provider.get_sim_time_converter(frame_min, frame_max)
            frame_min = converter.convert(frame_min)
            frame_max = converter.convert(frame_max)
            x_range_name = 'x_plot_axis'
            apply_x_axis_offset(fig, frame_min, frame_max, x_range_name)

        color_selector = ColorSelectorFactory.create_instance(coloring_rule='unique')
        if self._case == 'best':
            color_selector.get_color()
        colors = [color_selector.get_color(y_label) for y_label in y_labels]

        source = StackedBarSource(data, y_labels, self._xaxis_type, x_label, converter)
        # reverse the order of y_labels to reverse the order in which bars are stacked.
        stacked_bar = fig.vbar_stack(list(reversed(y_labels)), x='start time',
                                     width='x_width_list', color=list(reversed(colors)),
                                     source=source.to_source(None))
        source.add_label_data_to_stacked_bar(stacked_bar)
        source.add_latency_data_to_stacked_bar(stacked_bar)

        fig.add_tools(
            HoverKeysFactory.create_instance('stacked_bar', target_objects).create_hover())

        # add legend (for each var in stacked bar)
        legend_items = [(bar.name, [bar]) for bar in stacked_bar]
        legend_items.reverse()
        legend = Legend(items=legend_items, location='bottom_left',
                        orientation='vertical', click_policy='mute')
        fig.add_layout(legend, 'below')

        return fig


class StackedBarSource:
    """Class to generate stacked bar source."""

    def __init__(
        self,
        data: dict[str, list[int | float]],
        y_labels: list[str],
        xaxis_type: str,
        x_label: str,
        converter: ClockConverter | None
    ) -> None:
        x_width_list: list[float] = []

        # Convert the data unit to second
        data = self._updated_with_unit(data, y_labels, 1e-6, None)
        data = self._updated_with_unit(data, ['start time'], 1e-9, converter)

        # Calculate the stacked y values
        for prev_, next_ in zip(reversed(y_labels[:-1]), reversed(y_labels[1:])):
            data[prev_] = [data[prev_][i] + data[next_][i] for i in range(len(data[next_]))]

        if xaxis_type == 'system_time' or xaxis_type == 'sim_time':
            # Update the timestamps from absolutely time to offset time
            data[x_label] = self._updated_timestamps_to_offset_time(
                data[x_label], None)

            x_width_list = self._get_x_width_list(data[x_label], None)
            half_width_list = [x / 2 for x in x_width_list]

            # Slide x axis values so that the bottom left of bars are the start time.
            data[x_label] = self._add_shift_value(data[x_label], half_width_list)
        else:  # index
            data[x_label] = list(range(0, len(data[y_labels[0]])))
            x_width_list = self._get_x_width_list(data[x_label], None)

        self._data: dict[str, list[int | float]] = data
        self._x_width_list: list[float] = x_width_list

    def _updated_with_unit(
        self,
        data: dict[str, list[int | float]],
        columns: list[str] | None,
        unit: float,
        converter: ClockConverter | None
    ) -> dict[str, list[float]]:
        # TODO: make timeseries and callback scheduling function use this function.
        #       create bokeh_util.py
        if columns is None:
            output_data: dict[str, list[float]] = {}
            for key in data.keys():
                if not converter:
                    output_data[key] = [d * unit for d in data[key]]
                else:
                    output_data[key] = [converter.convert(d) * unit for d in data[key]]
        else:
            output_data = data
            for key in columns:
                if not converter:
                    output_data[key] = [d * unit for d in data[key]]
                else:
                    output_data[key] = [converter.convert(d) * unit for d in data[key]]
        return output_data

    def _get_x_width_list(self, x_values: list[float], converter: ClockConverter | None) -> list[float]:
        """
        Get width between a x value and next x value.

        Parameters
        ----------
        x_values : list[float]
            X values list.

        Returns
        -------
        list[float]
            Width list.

        """
        # TODO: create bokeh_util.py and move this.
        if not converter:
            x_width_list: list[float] = \
                [(x_values[i+1]-x_values[i]) * 0.99 for i in range(len(x_values)-1)]
        else:
            x_width_list: list[float] = \
                [(converter.convert(x_values[i+1])-converter.convert(x_values[i])) * 0.99 for i in range(len(x_values)-1)]
        x_width_list.append(x_width_list[-1])
        return x_width_list

    def _add_shift_value(
        self,
        values: list[float],
        shift_values: list[float]
    ) -> list[float]:
        """
        Add shift values to target values.

        Parameters
        ----------
        values : list[float]
            Target values.
        shift_values : list[float]
            Shift values

        Returns
        -------
        list[float]
            Updated values.

        """
        # TODO: create bokeh_util.py and move this.
        return [values[i] + shift_values[i] for i in range(len(values))]

    def _updated_timestamps_to_offset_time(
        self,
        x_values: list[float],
        converter: ClockConverter | None
    ):
        new_values: list[float] = []
        first_time = x_values[0]
        for time in x_values:
            if not converter:
                new_values.append(time - first_time)
            else:
                new_values.append(converter.convert(time) - converter.convert(first_time))
        return new_values

    def add_label_data_to_stacked_bar(self, stacked_bar: list[GraphRenderer]):
        # add 'label' data to each bar due to display hover
        x_len = min([len(v) for v in self._data.values()])
        for bar in stacked_bar:
            bar.data_source.add([bar.name] * x_len, 'label')

    def add_latency_data_to_stacked_bar(self, stacked_bar: list[GraphRenderer]):
        # add 'latency' data to each bar due to display hover
        for bar in stacked_bar:
                bar.data_source.add(['latency = ' + str(latency)
                                    for latency in self._data[bar.name]], 'latency')

    def to_source(
        self,
        converter: ClockConverter | None
    ) -> dict[str, list[int | float]]:
        # NOTE: Using `ColumnDataSource`, it is not possible
        # NOTE: to display a different hover for each stack (cause unknown).
        # convert timestamp to latency
        loc.loc()
        labels = list(self._data.keys())
        for k in self._data.keys():
            if k == 'start time':
                continue
            if labels[labels.index(k)+1] == 'start time':
                continue
            target_data = self._data[k]
            below_data = self._data[labels[labels.index(k)+1]]
            if converter:
                target_data = [converter.convert(item) for item in target_data]
                below_data = [converter.convert(item) for item in below_data]
            self._data[k] = [
                target - below for target, below in
                zip(target_data, below_data)
            ]
            print(f"--- {k}: {len(self._data[k])=}")
            for i in range(10):
                print(f"  {self._data[k][i+10]=} = {target_data[i+10]=} - {below_data[i+10]=}")

        # set data used in stacked bar
        source = self._data
        source['x_width_list'] = self._x_width_list

        return source
