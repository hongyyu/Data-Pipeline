from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from plugins import operators
from plugins import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "my_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
