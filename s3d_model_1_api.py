from fastapi import FastAPI, Query, HTTPException
from google.cloud import bigquery

app = FastAPI(title="S3D Models API")

client = bigquery.Client()

@app.get("/s3d_model_1/metrics")
def get_dependency_metrics(
    dependency_id: str = Query(..., description="Dependency ID to look up")
):
    """
    Retrieve relative_distribution and percentile_rank
    for the most recent run_date of a given dependency_id.
    """

    query = """
        SELECT relative_distribution, percentile_rank, run_date
        FROM `my_dataset.s3d_model_1`
        WHERE dependency_id = @dependency_id
        ORDER BY run_date DESC
        LIMIT 1
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dependency_id", "STRING", dependency_id)
            ]
        )
    )

    results = list(job)

    if not results:
        raise HTTPException(status_code=404, detail="Dependency ID not found")

    row = results[0]
    return {
        "dependency_id": dependency_id,
        "relative_distribution": row["relative_distribution"],
        "percentile_rank": row["percentile_rank"],
        "run_date": str(row["run_date"])
    }