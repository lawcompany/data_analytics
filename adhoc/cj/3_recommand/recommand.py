import os
import numpy as np
import pandas as pd
import prefect


from datetime import datetime, timedelta
from lightfm import LightFM
from numpy import ndarray
from prefect import task, Flow
from prefect.engine.results.local_result import LocalResult
from prefect.run_configs import LocalRun
from prefect.schedules import clocks, Schedule
from prefect.storage import Local
from prefect.tasks.gcp.bigquery import BigQueryTask, CreateBigQueryTable
from prefect.utilities import logging

from typing import Dict, List, Tuple


logger = logging.get_logger("logger")
TOPK = 15

bq_reconcile_table = CreateBigQueryTable(
    name="bq_reconcile_table",
    log_stdout=True,
    max_retries=5,
    retry_delay=timedelta(minutes=1),
)

bq_update_view = BigQueryTask(
    name="bq_update_view",
    log_stdout=True,
    max_retries=5,
    retry_delay=timedelta(minutes=1),
)


# Get parameter inputs for Tasks
@task
def get_bq_update_view_inputs() -> Tuple[str, str, str, str, str]:

    dataset = "inference"
    view = "recommend_qna"
    table_dataset = BIGQUERY_TABLE_CONFIGS["recommend_qna"].dataset
    table = BIGQUERY_TABLE_CONFIGS["recommend_qna"].table
    inferenced_at = prefect.context.get(
        "scheduled_start_time", datetime.utcnow()
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )  # Note that utcnow() is used as a workaround for register time reference

    return (dataset, view, table_dataset, table, inferenced_at)


# Render BQ queries
@task
def render_bq_update_view_query(
    dataset: str, view: str, table_dataset: str, table: str, inferenced_at: str
) -> str:
    rendered_query = f"""
        CREATE OR REPLACE VIEW {dataset}.{view}
        OPTIONS(
            friendly_name="",
            description=""
        ) AS
            SELECT
                *
            FROM
                {table_dataset}.{table}
            WHERE
                inferenced_at = TIMESTAMP'{inferenced_at}'
    """
    logger.info(rendered_query)
    return rendered_query


bq_delete_prev_inference_results = BigQueryTask(
    name="bq_delete_prev_inference_results",
    log_stdout=True,
    max_retries=5,
    retry_delay=timedelta(minutes=1),
)


def batch(iterable, batch_size):
    total_len = len(iterable)
    for ndx in range(0, total_len, batch_size):
        yield iterable[ndx : min(ndx + batch_size, total_len)]  # noqa: E203


def get_pool_by_category(
    query: int, qna2categories: Dict[int, str], category_items: Dict[str, int]
):
    """
    query값으로 받은 qna가 속해있는 카테고리들에게 속해있는 qna pool (list)를 리턴하는 함수
    E.g.)
    qna1 : [상속, 부동산, 재산]
    qna2 : [상속, 부동산]
    qna3 : [부동산]
    qna4 : [상속, 재산]
    qna4 => [qna1, qna2, qna4]
    """
    logger.info(f"Retrieving qna pool for: {query}")

    pool = []
    categories = qna2categories.get(query, [])

    for cat in categories:
        pool += category_items[cat]

    return pool


def get_similar_tags(tag_embeddings: ndarray, tag_id, topk=200):
    logger.info("Getting similar tags")
    logger.info(f"{tag_embeddings.shape}")
    query_embedding = tag_embeddings[tag_id]

    logger.info("Got query_embedding")
    logger.info(f"{query_embedding.shape}")
    similarity = query_embedding @ tag_embeddings.T

    logger.info("Calculated similarity")
    logger.info(f"{similarity.shape}")
    most_similar = similarity.argpartition(-topk, axis=1)[:, ::-1][:, :topk]
    return most_similar


def recommend(itemid: int, items: List[int], inferenced_at: datetime, topk=TOPK):
    result = []
    rank = 0
    for rec_id in items:
        if itemid == rec_id:
            continue
        result.append(
            {
                "id": int(itemid),
                "rec_id": int(rec_id),
                "rank": int(rank),
                "inferenced_at": inferenced_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        rank += 1
    return result[:topk]


load_query_count = BigQueryTask(
    name="load_query_count", to_dataframe=True, result=LocalResult()
)


@task(result=LocalResult())
def map_index_2_qna(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict, Dict]:
    upi2index = {}
    index2upi = {}
    qna2index = {}
    index2qna = {}

    for i, upi in enumerate(df["user_pseudo_id"].unique()):
        upi2index[upi] = i
        index2upi[i] = upi

    for i, qna in enumerate(df["n_qna"].unique()):
        qna2index[qna] = i
        index2qna[i] = int(qna)

    df["index"] = df["user_pseudo_id"].apply(upi2index.get)
    df["qna"] = df["n_qna"].apply(qna2index.get)
    df = df[["index", "qna", "cnt"]]
    df = df.astype(np.int32)

    return df, qna2index, index2qna


@task(result=LocalResult())
def get_qna_mapper(qna_categories: pd.DataFrame) -> Tuple[Dict, Dict]:
    """
    MAP<qna_id, List[qna_id]>를 미리 만들어두고 활용하기에는 데이터 규모가 너무 크기에 (18Gb+),
    두 가지 map을 만들어서 runtime에 O(1) lookup을 잘 활용하는 방안을 사용한다.
    """
    # MAP<category_id, List[qna_id]>
    category_items = {}
    for category_id, group in qna_categories.explode("category_id").groupby(
        "category_id"
    ):
        category_items[category_id] = group["qna_id"].tolist()

    # MAP<qna_id, List[category_id]>
    qna2categories = {}
    for _, row in qna_categories.iterrows():
        qna2categories[row["qna_id"]] = row["category_id"].tolist()

    return category_items, qna2categories


@task(result=LocalResult())
def get_matrix(df):
    import scipy.sparse as sp

    implicit_matrix = sp.lil_matrix((df["index"].max() + 1, df["qna"].max() + 1))
    for row in df.itertuples(index=False):
        index = getattr(row, "index")
        qna = getattr(row, "qna")
        cnt = getattr(row, "cnt")
        implicit_matrix[index, qna] = cnt
    implicit_csr = implicit_matrix.tocsr()
    return implicit_csr


@task(result=LocalResult())
def train_model(implicit_csr):
    # config :
    no_components = 75
    learning_rate = 0.05
    epoch = 1

    model: LightFM = LightFM(
        no_components=no_components, learning_rate=learning_rate, loss="bpr"
    )
    model.fit(implicit_csr, epochs=epoch, verbose=True)
    _item_biases, item_embedding = model.get_item_representations()
    tag_embeddings = (item_embedding.T / np.linalg.norm(item_embedding, axis=1)).T
    return tag_embeddings


@task
def inference(tag_embeddings, index2qna, category_items, qna2categories):
    from google.cloud import bigquery

    bqclient = bigquery.Client()

    PROJECT = "lawtalk-bigquery"
    DATASET = "raw"
    TABLE = "recommend_qna"
    table = bqclient.get_table(f"{PROJECT}.{DATASET}.{TABLE}")

    get_qna_by_index = np.vectorize(lambda x: int(index2qna[x]))

    for i, b in enumerate(batch(np.arange(tag_embeddings.shape[0]), batch_size=1000)):
        logger.info(f"Running batch {i}")
        result = get_similar_tags(tag_embeddings, b)
        query = get_qna_by_index(b)
        result = get_qna_by_index(result)
        recs = []
        for q, r in zip(query, result):
            pool = get_pool_by_category(q, category_items, qna2categories)

            tmp_r = list(np.intersect1d(r, pool))
            if len(tmp_r) > TOPK:
                r = tmp_r
            recs.extend(
                recommend(
                    itemid=q,
                    items=r,
                    inferenced_at=prefect.context.get("scheduled_start_time"),
                )
            )
        query = list(map(str, query))

        logger.info("Inserting new results")
        errors = bqclient.insert_rows_json(table, recs)
        if errors == []:
            logger.info("success")


if __name__ == "<flow>":
    logger.info("Registering flows")

    schedule = Schedule(
        clocks=[
            clocks.CronClock(
                cron="0 0/6 * * *",
                start_date=datetime(2021, 12, 12, 0),
                parameter_defaults={},
            )
        ]
    )

    with Flow(
        "recommend-qna",
        run_config=LocalRun(
            labels=[
                os.environ.get("SERVER_ENV"),
            ]
        ),
        storage=Local(),
        schedule=schedule,
        # state_handlers=[slack_notifier(ignore_states=[Running, Scheduled, Success])],
    ) as flow:

        query = """
        WITH qna_events AS (
            SELECT
                user_pseudo_id,
                event_timestamp,
                udfs.param_value_by_key('page_location',event_params).string_value as page_location
            FROM
                `analytics_265523655.*`
            WHERE TRUE
                AND udfs.param_value_by_key('page_location',event_params).string_value like '%/qna/%'
                AND udfs.param_value_by_key('page_location',event_params).string_value not like '%compose%'
                AND udfs.param_value_by_key('page_location',event_params).string_value not like '%complete%'
        ), qna_counts AS (
            SELECT
                user_pseudo_id, count(distinct n_qna) as cnt
            FROM (
                SELECT
                    user_pseudo_id, REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') as n_qna
                FROM
                    qna_events
                WHERE
                    REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') is not null
            )
            GROUP BY 1
        )
        SELECT
            b.*
        FROM
            qna_counts a
            LEFT JOIN (
                SELECT
                    user_pseudo_id, n_qna, count(1) AS cnt
                FROM (
                    SELECT
                        user_pseudo_id, REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') as n_qna
                    FROM
                        qna_events
                    WHERE
                        REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') is not null
                )
                GROUP BY
                    1, 2
            ) b ON a.user_pseudo_id = b.user_pseudo_id
        WHERE
            a.cnt >= 5
        """

        qna_query = """
        SELECT
            CAST(SPLIT(slug,'-')[OFFSET(0)] AS BIGINT) AS qna_id,
            udfs.extract_object_ids(categories)        AS category_id,
            title                                      AS title
        FROM
            `lawtalk-bigquery.raw.questions`
        """

        reconcile_target_table = bq_reconcile_table(
            dataset=BIGQUERY_TABLE_CONFIGS["recommend_qna"].dataset,
            table=BIGQUERY_TABLE_CONFIGS["recommend_qna"].table,
            schema=BIGQUERY_TABLE_CONFIGS["recommend_qna"].schema,
        )

        bq_delete_prev_inference_results = bq_delete_prev_inference_results(
            query="""
            DELETE
            FROM
                {dataset}.{table}
            WHERE
                inferenced_at = TIMESTAMP'{inferenced_at}'
            """.format(
                dataset=BIGQUERY_TABLE_CONFIGS["recommend_qna"].dataset,
                table=BIGQUERY_TABLE_CONFIGS["recommend_qna"].table,
                inferenced_at=prefect.context.get("date", datetime.utcnow()).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # Note that utcnow() is used as a workaround for register time reference
            ),
        )

        df = load_query_count(query=query)
        df.set_upstream(reconcile_target_table)

        qna_categories = load_query_count(query=qna_query)

        df, qna2index, index2qna = map_index_2_qna(df)
        category_items, qna2categories = get_qna_mapper(qna_categories)

        implicit_csr = get_matrix(df)
        tag_embeddings = train_model(implicit_csr)
        inf = inference(tag_embeddings, index2qna, category_items, qna2categories)

        inf.set_upstream(bq_delete_prev_inference_results)

        (
            dataset,
            view,
            table_dataset,
            table,
            inferenced_at,
        ) = get_bq_update_view_inputs()

        rendered_query = render_bq_update_view_query(
            dataset=dataset,
            view=view,
            table_dataset=table_dataset,
            table=table,
            inferenced_at=inferenced_at,
        )

        bq_update_view = bq_update_view(query=rendered_query)

        bq_update_view.set_upstream(inf)
        