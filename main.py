from fastapi import FastAPI, Query, HTTPException
from typing import List, Union, Optional
from google.cloud import bigquery

app = FastAPI(title="S3D Models API")

# BigQuery client
client = bigquery.Client()


# -----------------------
# Health Check
# -----------------------
@app.get("/")
def health_check():
    return {"status": "ok", "service": "s3d-models-api"}


# -----------------------
# s3d_model_1 Endpoint 
# -----------------------
@app.get("/s3d_model_1/metrics")
def get_dependency_metrics_model1(
    dependency_ids: Union[str, List[str]] = Query(..., description="One or more dependency IDs")
):
    """
    Retrieve relative_distribution and percentile_rank
    for the most recent run_date(s) of one or more dependency_ids (s3d_model_1).
    """

    if isinstance(dependency_ids, str):
        dependency_ids = [dependency_ids]

    query = """
        WITH ranked AS (
            SELECT
                dependency_id,
                relative_distribution,
                percentile_rank,
                run_date,
                ROW_NUMBER() OVER (PARTITION BY dependency_id ORDER BY run_date DESC) AS rn
            FROM `s3d_dura_data.s3d_model_1`
            WHERE dependency_id IN UNNEST(@dependency_ids)
        )
        SELECT dependency_id, relative_distribution, percentile_rank, run_date
        FROM ranked
        WHERE rn = 1
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("dependency_ids", "STRING", dependency_ids)
            ]
        )
    )

    results = [dict(row) for row in job]

    if not results:
        raise HTTPException(status_code=404, detail="No matching dependency IDs found")

    for r in results:
        r["run_date"] = str(r["run_date"])

    return results


# -----------------------
# s3d_model_2 Endpoint 
# -----------------------
@app.get("/s3d_model_2/metrics")
def get_dependency_metrics_model2(
    dependency_names: Union[str, List[str]] = Query(..., description="One or more dependency names"),
    dependency_versions: Optional[Union[str, List[str]]] = Query(
        None, description="Optional versions (must align with names if provided)"
    )
):
    """
    Returns BOTH sets of metrics:
      - relative_distribution, percentile_rank           (name-level)
      - relative_distribution_version, percentile_rank_version (version-level)
    Always returns the latest run_date per dependency.
    """

    # Normalize inputs
    if isinstance(dependency_names, str):
        dependency_names = [dependency_names]
    if dependency_versions and isinstance(dependency_versions, str):
        dependency_versions = [dependency_versions]

    if dependency_versions and len(dependency_names) != len(dependency_versions):
        raise HTTPException(
            status_code=400,
            detail="If dependency_versions are provided, they must match the number of dependency_names.",
        )

    # -----------------------
    # Query if versions provided
    # -----------------------
    if dependency_versions:
        pairs = [(n, v) for n, v in zip(dependency_names, dependency_versions)]

        query = """
            WITH ranked AS (
                SELECT
                    dependency_name,
                    dependency_version,
                    -- name-level metrics
                    relative_distribution_name AS relative_distribution,
                    percentile_rank_name       AS percentile_rank,
                    -- version-level metrics
                    relative_distribution_version AS relative_distribution_version,
                    percentile_rank_version       AS percentile_rank_version,
                    run_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY dependency_name, dependency_version
                        ORDER BY run_date DESC
                    ) AS rn
                FROM `s3d_dura_data.s3d_model_2`
                WHERE (dependency_name, dependency_version) IN UNNEST(@pairs)
            )
            SELECT dependency_name, dependency_version,
                   relative_distribution, percentile_rank,
                   relative_distribution_version, percentile_rank_version,
                   run_date
            FROM ranked
            WHERE rn = 1
        """

        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter(
                        "pairs",
                        "STRUCT<dependency_name STRING, dependency_version STRING>",
                        pairs,
                    )
                ]
            ),
        )

    # -----------------------
    # Query if only names provided
    # -----------------------
    else:
        query = """
            WITH ranked AS (
                SELECT
                    dependency_name,
                    CAST(NULL AS STRING) AS dependency_version,
                    relative_distribution_name AS relative_distribution,
                    percentile_rank_name       AS percentile_rank,
                    CAST(NULL AS FLOAT64) AS relative_distribution_version,
                    CAST(NULL AS FLOAT64) AS percentile_rank_version,
                    run_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY dependency_name
                        ORDER BY run_date DESC
                    ) AS rn
                FROM `s3d_dura_data.s3d_model_2`
                WHERE dependency_name IN UNNEST(@dependency_names)
            )
            SELECT dependency_name, dependency_version,
                   relative_distribution, percentile_rank,
                   relative_distribution_version, percentile_rank_version,
                   run_date
            FROM ranked
            WHERE rn = 1
        """

        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("dependency_names", "STRING", dependency_names)
                ]
            ),
        )

    # -----------------------
    # Results
    # -----------------------
    results = [dict(row) for row in job]

    if not results:
        raise HTTPException(status_code=404, detail="No matching dependencies found")

    for r in results:
        r["run_date"] = str(r["run_date"])

    return results