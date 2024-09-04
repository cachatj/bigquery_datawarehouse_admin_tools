import io
import logging
import time
import zipfile

from looker_sdk import models40 as mdls
from pathvalidate import sanitize_filename
from typing import Dict

import json

logger = logging.getLogger(__name__)


class DashboardDownloader:
    def __init__(self, sdk, dashboard_id="", dashboard_filters=None, limit=-1):

        self._dashboard_id = dashboard_id
        self._dashboard_filters = dashboard_filters
        self._sdk = sdk #looker_sdk.init40()

        self.dashboard = self._dashboard_from_sdk(dashboard_id)
        self._tasks = []
        self._queries = []
        self._task_results = []
        self._limit = limit
        self._create_queries()

    @property
    def dashboard_id(self):
        return self._dashboard_id

    @dashboard_id.setter
    def dashboard_id(self, value):
        self._dashboard_id = value

    @property
    def dashboard_filters(self):
        return self._dashboard_filters

    @dashboard_filters.setter
    def dashboard_filters(self, value: str):
        self._dashboard_filters = value

    def _dashboard_from_sdk(self, slug: str):
        try:
            return self._sdk.dashboard(slug)
        except Exception as e:
            raise ValueError("Dashboard could not be found") from e

    def _create_element_filters(self, element, default_filters, dashboard_filters):
        element_filters = {}
        for filters in element.result_maker.filterables:
            for option in filters.listen:
                element_filters[option.dashboard_filter_name] = option.field
        
        dd_filters={}
        for default_filter in default_filters:
            dd_filters[default_filter.name]=default_filter.default_value
        print(dd_filters)
        print(dashboard_filters)
        merged_filters = dd_filters
        merged_filters.update(dashboard_filters)
        print(merged_filters)

        new_filters = {}
        for key, value in element_filters.items():
            check_filter= merged_filters.get(key)
            if isinstance(check_filter, str):
                new_filters[value]=check_filter
            elif len(check_filter)==1:
                new_filters[value]=check_filter
            elif len(check_filter)>1:
                new_filters[value] = ",".join(check_filter)
        print(new_filters)
        return new_filters

    def _create_query(self, dashboard_element):
        element_query_filters = self._create_element_filters(
            dashboard_element, self.dashboard.dashboard_filters, self._dashboard_filters
        )
        q = dashboard_element.result_maker.query.__dict__
        no_limit = True
        for key in [
            "can",
            "id",
            "slug",
            "share_url",
            "expanded_share_url",
            "url",
            "has_table_calculations",
            "_converter",
            "client_id",
        ]:
            q.pop(key, "")

        if no_limit:
            q.pop("limit", "")

        q["filters"] = element_query_filters

        updatedQuery = mdls.WriteQuery(**q)

        try:
            created_query = self._sdk.create_query(body=updatedQuery)
            return created_query
        except Exception as e:
            raise (e)

    def _create_queries(self):
        queries = []
        for element in self.dashboard.dashboard_elements:
            if element.type == 'vis':
                tile_title = sanitize_filename(element.title)
                queries.append({"name": tile_title, "query": self._create_query(element)})
        # pylint: disable=attribute-defined-outside-init
        self._queries = queries

    def create_tasks(self):
        tasks = []
        for query in self._queries:
            task = self._sdk.create_query_task(
                body=mdls.CreateQueryTask(
                    query_id=query.get("query").get("id"), result_format="xlsx"
                ),
                apply_formatting=True,
                apply_vis=True,
                cache=True,
                server_table_calcs=True,
                limit = self._limit,
            )
            tasks.append({"name": query.get("name"), "task": task.id})
        # pylint: disable=attribute-defined-outside-init
        self._tasks = tasks

    def _get_query_result(self, task_id: str):
        print('TID',task_id)
        elapsed = 0.0
        delay = 0.5  # wait .5 seconds
        while True:
            poll = self._sdk.query_task(query_task_id=task_id)
            if poll["status"] == "error":
                #print(poll)
                # pylint: disable=broad-exception-raised
                raise Exception("Query failed")
            elif poll["status"] == "complete":
                break
            time.sleep(delay)
            elapsed += delay
        logger.info("query task completed in %s seconds", elapsed)
        return self._sdk.query_task_results(task_id)

    def get_tasks_results(self):
        task_results = []
        for task in self._tasks:
            task_results.append(
                {"name":task.get("name"), "file":self._get_query_result(task.get("task"))}
            )

        # pylint: disable=attribute-defined-outside-init
        self._task_results = task_results
        return task_results


def create_zip_file(xlsx_files: Dict)-> io.BytesIO:
    """Create a Zip File out of a dictionary containing names and a BytesIO object

    Args:
        xlsx_files (Dict): Object with filename and BytesIO Object

    Returns:
        io.BytesIO: _description_
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for xlsx_file in xlsx_files:
            zip_file.writestr(f"{xlsx_file.get("name")}.xlsx", xlsx_file.get("file"))
    return zip_buffer

