from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional
from google.cloud import bigquery

app = FastAPI(title="S3D Models API")

client = bigquery.Client()

@app.get("/s3d_model_2/metrics")
def get_dependency_metrics_model2(
    dependency_names: List[str] = Query(..., description="One or more dependency names"),
    dependency_versions: Optional[List[str]] = Query(None, description="Optional versions (must align with names if provided)")
):
    """
    Retrieve relative distribution and percentile rank for dependency name or (name+version)
    using the most recent run_date(s).
    """

    if dependency_versions and len(dependency_names) != len(dependency_versions):
        raise HTTPException(
            status_code=400,
            detail="If dependency_versions are provided, they must match the number of dependency_names."
        )

    if dependency_versions:
        # Query by name + version pairs
        query = """
            WITH ranked AS (
                SELECT
                    dependency_name,
                    dependency_version,
                    relative_distribution_version AS relative_distribution,
                    percentile_rank_version AS percentile_rank,
                    run_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY dependency_name, dependency_version
                        ORDER BY run_date DESC
                    ) AS rn
                FROM `my_dataset.s3d_model_2`
                WHERE (dependency_name, dependency_version) IN UNNEST(@pairs)
            )
            SELECT dependency_name, dependency_version, relative_distribution, percentile_rank, run_date
            FROM ranked
            WHERE rn = 1
        """

        pairs = list(zip(dependency_names, dependency_versions))
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("pairs", "STRUCT<dependency_name STRING, dependency_version STRING>", pairs)
                ]
            )
        )

    else:
        # Query by name only
        query = """
            WITH ranked AS (
                SELECT
                    dependency_name,
                    relative_distribution_name AS relative_distribution,
                    percentile_rank_name AS percentile_rank,
                    run_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY dependency_name
                        ORDER BY run_date DESC
                    ) AS rn
                FROM `my_dataset.s3d_model_2`
                WHERE dependency_name IN UNNEST(@dependency_names)
            )
            SELECT dependency_name, relative_distribution, percentile_rank, run_date
            FROM ranked
            WHERE rn = 1
        """

        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("dependency_names", "STRING", dependency_names)
                ]
            )
        )

    results = [dict(row) for row in job]

    if not results:
        raise HTTPException(status_code=404, detail="No matching dependencies found")

    # Convert run_date to string for JSON
    for r in results:
        r["run_date"] = str(r["run_date"])

    return results