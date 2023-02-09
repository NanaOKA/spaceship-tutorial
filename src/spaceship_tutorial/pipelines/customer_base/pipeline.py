"""
This is a boilerplate pipeline 'customer_base'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import run_sql_queries, customer_spend_aggregates

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=run_sql_queries,
                inputs=["customer_base_sql_script", "params:sql_queries", "params:redshift"],
                outputs=None,
                name="generate_customer_base_node",
            ),
            node(
                func=run_sql_queries,
                inputs=["customer_base_sql_script", "params:na_sql_queries", "params:redshift"],
                outputs=None,
                name="generate_na_customer_base_node",
            ),
            node(
                func=customer_spend_aggregates,
                inputs=["na_customer_spend"],
                outputs="na_customer_spend_agg",
                name="generate_na_spend_agg",
            ),
        ]
    )